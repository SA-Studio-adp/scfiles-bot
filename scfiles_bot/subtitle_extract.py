"""
subtitle_extract.py — core extraction logic used by subs_bot.py.

Two extraction paths:
  1. Embedded subtitle tracks (mkv/mp4/etc internal streams) — via ffmpeg/
     ffprobe. Fast, reliable, always attempted.
  2. Hardcoded / burned-in subtitles — best-effort OCR over sampled frames
     via Tesseract. Only runs if SUBS_OCR_ENABLED=1 (see config below), off
     by default: it's slow (minutes per video), CPU-heavy, and noticeably
     less accurate than embedded extraction. Treat it as a fallback for
     videos with NO embedded tracks, not a primary path.

Both paths write .srt files named "<basename>.<lang>.srt" into the given
workdir and return their paths — the caller (subs_bot.py) is responsible
for sending them and for deleting the whole workdir afterwards.
"""
import asyncio, json, os, re, shutil

from config import logger

SUBS_OCR_ENABLED = os.environ.get("SUBS_OCR_ENABLED", "0").strip() == "1"
# How often to sample frames for OCR (seconds). Lower = more accurate
# timing / more duplicate lines merged, but slower.
OCR_SAMPLE_INTERVAL = float(os.environ.get("SUBS_OCR_INTERVAL", "1.0"))


class ExtractionError(Exception):
    pass


async def _run(*args: str, timeout: int = 600) -> tuple[int, bytes, bytes]:
    proc = await asyncio.create_subprocess_exec(
        *args, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE)
    try:
        out, err = await asyncio.wait_for(proc.communicate(), timeout=timeout)
    except asyncio.TimeoutError:
        proc.kill()
        await proc.wait()
        raise ExtractionError(f"'{args[0]}' timed out after {timeout}s")
    return proc.returncode, out, err


def _check_tools():
    missing = [t for t in ("ffmpeg", "ffprobe") if shutil.which(t) is None]
    if missing:
        raise ExtractionError(
            f"Required tool(s) not installed on server: {', '.join(missing)}. "
            "Install ffmpeg (provides both) — see Dockerfile."
        )


# ── embedded subtitle tracks ────────────────────────────────────────────
async def list_subtitle_streams(video_path: str) -> list[dict]:
    """Returns [{'index': int, 'lang': str, 'codec': str}, ...] for every
    subtitle stream in the file, via ffprobe. 'lang' falls back to 'und'
    (undefined) when the file has no language tag on that track."""
    _check_tools()
    rc, out, err = await _run(
        "ffprobe", "-v", "error", "-select_streams", "s",
        "-show_entries", "stream=index,codec_name:stream_tags=language",
        "-of", "json", video_path,
    )
    if rc != 0:
        raise ExtractionError(f"ffprobe failed: {err.decode(errors='ignore')[:300]}")
    data = json.loads(out or b"{}")
    streams = []
    for i, s in enumerate(data.get("streams", [])):
        lang = (s.get("tags") or {}).get("language", "und").strip() or "und"
        streams.append({
            "rel_index": i,           # position among subtitle streams (0-based) — what -map 0:s:N wants
            "lang": lang,
            "codec": s.get("codec_name", "unknown"),
        })
    return streams


# Bitmap subtitle codecs (dvd_subtitle, hdmv_pgs_subtitle/PGS, dvb_subtitle)
# can't be converted to SRT with ffmpeg — they're images, not text.
_TEXT_CODECS_UNSUPPORTED = {"dvd_subtitle", "hdmv_pgs_subtitle", "dvb_subtitle"}


async def extract_embedded_subs(video_path: str, basename: str, workdir: str) -> tuple[list[str], list[str]]:
    """Extracts every text-based embedded subtitle track to
    '<workdir>/<basename>.<lang>.srt'. Returns (srt_paths, skipped_notes)
    — skipped_notes describes tracks that exist but couldn't be converted
    (e.g. image-based PGS/VOBSUB subs)."""
    streams = await list_subtitle_streams(video_path)
    srt_paths: list[str] = []
    skipped: list[str] = []
    lang_seen: dict[str, int] = {}

    for s in streams:
        lang = s["lang"]
        if s["codec"] in _TEXT_CODECS_UNSUPPORTED:
            skipped.append(f"{lang} ({s['codec']} is image-based, not text — can't convert to SRT)")
            continue

        # de-dupe: video.en.srt, video.en-2.srt, video.en-3.srt, ...
        lang_seen[lang] = lang_seen.get(lang, 0) + 1
        tag = lang if lang_seen[lang] == 1 else f"{lang}-{lang_seen[lang]}"
        out_path = os.path.join(workdir, f"{basename}.{tag}.srt")

        rc, _, err = await _run(
            "ffmpeg", "-y", "-i", video_path,
            "-map", f"0:s:{s['rel_index']}", "-c:s", "srt", out_path,
        )
        if rc != 0 or not os.path.exists(out_path) or os.path.getsize(out_path) == 0:
            skipped.append(f"{lang} ({s['codec']}) — ffmpeg conversion failed")
            if os.path.exists(out_path):
                os.remove(out_path)
            continue
        srt_paths.append(out_path)

    return srt_paths, skipped


# ── hardcoded/burned-in subtitles via OCR (optional, best-effort) ──────
_SRT_TIME = lambda secs: (
    f"{int(secs//3600):02d}:{int(secs%3600//60):02d}:{int(secs%60):02d},{int(secs%1*1000):03d}"
)


async def extract_hardcoded_subs_ocr(video_path: str, basename: str, workdir: str,
                                       on_progress=None) -> str | None:
    """Best-effort OCR pass over the bottom third of sampled frames.
    Returns the path to '<basename>.ocr.srt', or None if nothing was
    detected / pytesseract isn't installed / OCR is disabled.

    on_progress, if given, is a plain (non-async) callable(done_frames,
    total_frames) invoked periodically from a worker thread — the whole
    scan runs via asyncio.to_thread since cv2/tesseract are blocking, so
    a caller can poll shared state from a separate coroutine to render a
    progress bar without freezing the event loop for the whole scan."""
    if not SUBS_OCR_ENABLED:
        return None
    try:
        import cv2
        import pytesseract
    except ImportError:
        logger.warning("SUBS_OCR_ENABLED=1 but opencv-python-headless/pytesseract not installed — skipping OCR")
        return None
    if shutil.which("tesseract") is None:
        logger.warning("SUBS_OCR_ENABLED=1 but the tesseract binary isn't installed — skipping OCR")
        return None

    return await asyncio.to_thread(
        _ocr_sync, video_path, basename, workdir, on_progress)


def _ocr_sync(video_path: str, basename: str, workdir: str, on_progress) -> str | None:
    import cv2
    import pytesseract

    cap = cv2.VideoCapture(video_path)
    fps = cap.get(cv2.CAP_PROP_FPS) or 25.0
    frame_count = int(cap.get(cv2.CAP_PROP_FRAME_COUNT) or 0)
    duration = frame_count / fps if fps else 0
    step_frames = max(1, int(fps * OCR_SAMPLE_INTERVAL))

    entries: list[tuple[float, float, str]] = []  # (start, end, text)
    cur_text, cur_start = "", None
    t = 0.0
    idx = 0

    def _sample_and_ocr(frame) -> str:
        h, w = frame.shape[:2]
        crop = frame[int(h * 0.78):h, 0:w]  # bottom ~22% — typical subtitle band
        gray = cv2.cvtColor(crop, cv2.COLOR_BGR2GRAY)
        _, thresh = cv2.threshold(gray, 200, 255, cv2.THRESH_BINARY)
        text = pytesseract.image_to_string(thresh).strip()
        return re.sub(r"\s+", " ", text)

    while True:
        ok = cap.grab()
        if not ok:
            break
        if idx % step_frames == 0:
            ok, frame = cap.retrieve()
            if ok:
                text = _sample_and_ocr(frame)
                t = idx / fps
                if text and text == cur_text:
                    pass  # same line continuing, extend later
                elif text:
                    if cur_text:
                        entries.append((cur_start, t, cur_text))
                    cur_text, cur_start = text, t
                elif cur_text:
                    entries.append((cur_start, t, cur_text))
                    cur_text, cur_start = "", None
            if on_progress and frame_count:
                on_progress(idx, frame_count)
        idx += 1
    if cur_text:
        entries.append((cur_start, duration or t, cur_text))
    cap.release()
    if on_progress and frame_count:
        on_progress(frame_count, frame_count)

    if not entries:
        return None

    out_path = os.path.join(workdir, f"{basename}.ocr.srt")
    with open(out_path, "w", encoding="utf-8") as f:
        for i, (start, end, text) in enumerate(entries, 1):
            f.write(f"{i}\n{_SRT_TIME(start)} --> {_SRT_TIME(end)}\n{text}\n\n")
    return out_path
