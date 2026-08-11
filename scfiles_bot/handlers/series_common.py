"""handlers/series_common.py — helpers shared by handlers/series_add.py and
handlers/series_edit.py: episode-dict merging, subtitle parsing/formatting,
season summaries, link-input interpretation, and the season-picker keyboard."""
from telegram import InlineKeyboardButton, InlineKeyboardMarkup

from utils import code

def _ep_save(seasons: list, sn: int, epn: int, links: dict, subtitles: dict = None):
    season = next((s for s in seasons if s["season_number"]==sn), None)
    if not season:
        season = {"season_number": sn, "episodes": []}
        seasons.append(season)
        seasons.sort(key=lambda s: s["season_number"])
    season["episodes"] = [e for e in season["episodes"] if e["ep_number"]!=epn]
    season["episodes"].append({
        "ep_number": epn,
        "links":     links,
        "subtitles": subtitles or {},   # dict {lang: url} — same format as movies
    })
    season["episodes"].sort(key=lambda e: e["ep_number"])

def _parse_subtitles(text: str) -> dict:
    """Parse subtitle input (lang|url per line) into a dict.
    Bare URL with no lang prefix → defaults to 'en'.
    '-' → empty dict (no subtitles).
    """
    subs = {}
    if not text or text.strip() == "-":
        return subs
    for line in text.splitlines():
        line = line.strip()
        if not line:
            continue
        if "|" in line:
            lang, url = line.split("|", 1)
            lang, url = lang.strip().lower(), url.strip()
        else:
            lang, url = "en", line.strip()
        if lang and url:
            subs[lang] = url
    return subs

def _fmt_subtitles(subs: dict) -> str:
    """Format a subtitles dict back to lang|url lines (for edit prompts)."""
    if not subs:
        return "—"
    return " · ".join(subs.keys())

def _series_summary(seasons: list) -> str:
    total = sum(len(s["episodes"]) for s in seasons)
    lines = [f"  S{s['season_number']}: {len(s['episodes'])} ep(s)" for s in seasons]
    return f"📊 {len(seasons)} season(s) · {total} episode(s)\n" + "\n".join(lines)

def _format_ep_ranges(ep_numbers) -> str:
    """[13,14,15,16,20] -> '13-16, 20'. Single number stays bare ('5')."""
    nums = sorted(set(ep_numbers))
    if not nums:
        return ""
    ranges, start = [], nums[0]
    prev = start
    for n in nums[1:]:
        if n == prev + 1:
            prev = n
            continue
        ranges.append((start, prev))
        start = prev = n
    ranges.append((start, prev))
    return ", ".join(f"{a}-{b}" if a != b else f"{a}" for a, b in ranges)

def _session_upload_summary(session_episodes, orig_season_numbers):
    """Turns the (season, episode) pairs touched during an /editseries
    session into ("S1 . EP 13-16 has been uploaded" per touched season,
    joined by newline, is_new_season). Falls back to an empty line/False if
    nothing was tracked (shouldn't normally happen)."""
    by_season = {}
    for sn, epn in session_episodes:
        by_season.setdefault(sn, []).append(epn)
    is_new_season = any(sn not in orig_season_numbers for sn in by_season)
    lines = [f"S{sn} . EP {_format_ep_ranges(eps)} has been uploaded"
              for sn, eps in sorted(by_season.items())]
    return "\n".join(lines), is_new_season



def _link_input(t: str, current: str) -> str:
    """Interpret a link entry in the editseries quality steps.
    '-'                      -> keep the current value unchanged
    'clear' / 'remove' / 'none' -> remove the link
    anything else             -> use as the new link
    """
    t = t.strip()
    if t == "-":
        return current
    if t.lower() in ("clear", "remove", "none"):
        return ""
    return t

def _q_prompt(field_label: str, current: str) -> str:
    cur_line = f"\n<i>Current:</i> {code(current)}" if current else ""
    return (f"🎞 Enter <b>{field_label} link</b>{cur_line}\n"
            f"Or {code('-')} to keep/skip, {code('clear')} to remove:")

def _season_picker_kb(seasons: list) -> InlineKeyboardMarkup:
    rows, row = [], []
    for s in seasons:
        row.append(InlineKeyboardButton(f"S{s['season_number']}", callback_data=f"esssn_{s['season_number']}"))
        if len(row) == 5:
            rows.append(row); row = []
    if row: rows.append(row)
    rows.append([InlineKeyboardButton("➕ New Season", callback_data="esssn_new")])
    rows.append([InlineKeyboardButton("❌ Cancel", callback_data="ess_cancel")])
    return InlineKeyboardMarkup(rows)
