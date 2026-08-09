"""web/dashboard.py — the public "/" status dashboard, /health JSON,
/backup/all ZIP download, /logs raw tail, and the token gate shared with
web/admin_panel.py."""
import asyncio, io, os
from datetime import datetime

import aiohttp
from aiohttp import web

from config import state, IST, LOG_FILE, BACKEND_URL, BOT_WEB_URL, ADMIN_TOKEN
from api_client import sess, fetch_all
from backup import make_zip

def _check_token(req: web.Request) -> bool:
    auth = req.headers.get("Authorization","")
    if auth == f"Bearer {ADMIN_TOKEN}": return True
    return req.rel_url.query.get("token","") == ADMIN_TOKEN

def _token_required(req: web.Request) -> web.Response | None:
    if not _check_token(req):
        return web.Response(status=401,
                            text="401 Unauthorised\nPass ?token=<ADMIN_TOKEN> or Authorization: Bearer <token>")
    return None


async def web_dashboard(req: web.Request) -> web.Response:
    now    = datetime.now(IST)
    uptime = str(now - state.BOT_STARTED_AT).split(".")[0]

    async def ping_backend():
        try:
            s = await sess(); t0 = datetime.now()
            async with s.get(BACKEND_URL, timeout=aiohttp.ClientTimeout(total=10)) as r:
                return r.status, (datetime.now()-t0).total_seconds()*1000, ""
        except Exception as e:
            return None, 0.0, str(e)

    (b_code, b_ms, b_err), (movies, series, cols) = await asyncio.gather(
        ping_backend(), fetch_all())

    b_status  = "online" if b_code==200 else ("degraded" if b_code else "offline")
    bk  = state.LAST_BACKUP_AT.strftime("%d %b %Y, %H:%M IST") if state.LAST_BACKUP_AT else "Never"
    pg  = state.LAST_PING_AT.strftime("%H:%M:%S IST")           if state.LAST_PING_AT   else "Never"
    total_eps = sum(len(ep.get("episodes",[]))
                    for s in series for ep in s.get("seasons",[]))
    total_dls = sum(len(m.get("downloads",{})) for m in movies)
    lat_w = min(max(int(b_ms/8),4),100)
    lat_c = "#00e5a0" if b_ms<400 else ("#ffb340" if b_ms<1000 else "#ff4d6d")
    s_cls = "ok" if b_status=="online" else ("warn" if b_status=="degraded" else "bad")
    admin_url = f"/admin?token={ADMIN_TOKEN}"

    def mv_row(m):
        mid=m.get("id","?"); tmdb=m.get("tmdb_id","?")
        extras=m.get("extras","") or '<span class="dim">—</span>'
        dls=m.get("downloads",{})
        dl_html=('<span class="pill">'+" · ".join(str(k)+"p" for k in sorted(dls.keys()))+"</span>") if dls else '<span class="dim">—</span>'
        pos_h='<span class="pos-top">TOP</span>' if m.get("pos")=="top" else ""
        return f"<tr><td><code>{mid}</code></td><td><code>{tmdb}</code></td><td>{extras}</td><td>{dl_html}</td><td>{pos_h}</td></tr>"

    def sr_row(s):
        eps=sum(len(sn.get("episodes",[]))for sn in s.get("seasons",[]))
        return f"<tr><td><code>{s.get('id','?')}</code></td><td><code>{s.get('tmdb_id','?')}</code></td><td><b>{len(s.get('seasons',[]))}</b></td><td>{eps}</td></tr>"

    def co_row(k,v):
        return f"<tr><td><code>{k}</code></td><td><b>{v.get('name','?')}</b></td><td>{len(v.get('movies',[]))}</td></tr>"

    mv_rows = "".join(mv_row(m) for m in movies[:10])
    sr_rows = "".join(sr_row(s) for s in series[:10])
    co_rows = "".join(co_row(k,v) for k,v in list(cols.items())[:10])
    err_row = (f"<div class='srow'><span class='lbl'>Error</span><span class='bad' style='font-size:.72rem'>{b_err[:60]}</span></div>" if b_err else "")
    empty   = "<tr><td colspan='5' class='empty'>No data</td></tr>"

    css = ":root{--bg:#07080f;--s1:#0d1018;--s2:#12161f;--s3:#181d28;--border:#1e2535;--border2:#252d3d;--text:#e2e8f4;--muted:#4a5878;--dim:#2d3850;--ok:#00d98b;--warn:#f59e0b;--bad:#f43f5e;--accent:#6366f1;--accent2:#8b5cf6;--accent3:#06b6d4;--mono:'JetBrains Mono',monospace;--sans:'Syne',sans-serif;}*{box-sizing:border-box;margin:0;padding:0}body{background:var(--bg);color:var(--text);font-family:var(--sans);min-height:100vh;overflow-x:hidden}.blob{position:fixed;border-radius:50%;filter:blur(120px);pointer-events:none;z-index:0}.blob-1{width:600px;height:600px;top:-200px;left:-100px;background:radial-gradient(circle,rgba(99,102,241,.12),transparent 70%)}.blob-2{width:500px;height:500px;bottom:-100px;right:-100px;background:radial-gradient(circle,rgba(6,182,212,.08),transparent 70%)}header{display:flex;align-items:center;justify-content:space-between;padding:1.2rem 2rem;margin-bottom:1.5rem;background:rgba(13,16,24,.85);backdrop-filter:blur(20px);border-bottom:1px solid var(--border);position:sticky;top:0;z-index:100}.brand{display:flex;align-items:center;gap:.75rem}.logo-icon{width:36px;height:36px;border-radius:9px;background:linear-gradient(135deg,var(--accent),var(--accent2));display:flex;align-items:center;justify-content:center;font-size:1rem;box-shadow:0 0 20px rgba(99,102,241,.3)}.brand-name{font-size:1.05rem;font-weight:800;letter-spacing:-.5px}.brand-name span{color:var(--accent)}.brand-sub{font-size:.6rem;color:var(--muted);font-family:var(--mono)}.live-pill{display:flex;align-items:center;gap:5px;background:rgba(0,217,139,.08);border:1px solid rgba(0,217,139,.2);padding:3px 10px;border-radius:20px;font-size:.68rem;font-family:var(--mono);color:var(--ok)}.live-dot{width:6px;height:6px;border-radius:50%;background:var(--ok);animation:blink 2s infinite}@keyframes blink{0%,100%{opacity:1}50%{opacity:.3}}.wrap{position:relative;z-index:1;max-width:1200px;margin:0 auto;padding:0 1.5rem 2rem}.kpi-grid{display:grid;grid-template-columns:repeat(auto-fit,minmax(140px,1fr));gap:1rem;margin-bottom:1.5rem}.kpi{background:var(--s1);border:1px solid var(--border);border-radius:14px;padding:1.2rem;transition:all .2s}.kpi:hover{border-color:var(--border2);transform:translateY(-2px)}.kpi-lbl{font-size:.65rem;font-family:var(--mono);color:var(--muted);text-transform:uppercase;letter-spacing:1.5px}.kpi-val{font-size:2.2rem;font-weight:800;margin-top:.25rem;font-family:var(--mono);background:linear-gradient(135deg,var(--accent),var(--accent2));-webkit-background-clip:text;-webkit-text-fill-color:transparent}.kpi-sub{font-size:.65rem;color:var(--muted);margin-top:.15rem;font-family:var(--mono)}.panels{display:grid;grid-template-columns:1fr 1fr;gap:1rem;margin-bottom:1.5rem}@media(max-width:600px){.panels{grid-template-columns:1fr}}.panel{background:var(--s1);border:1px solid var(--border);border-radius:14px;padding:1.4rem}.panel-hdr{font-family:var(--mono);font-size:.68rem;text-transform:uppercase;letter-spacing:1.5px;color:var(--muted);margin-bottom:1.2rem;padding-bottom:.8rem;border-bottom:1px solid var(--border)}.srow{display:flex;justify-content:space-between;align-items:center;padding:.45rem 0;border-bottom:1px solid rgba(255,255,255,.03);font-size:.84rem}.srow:last-child{border-bottom:none}.lbl{color:var(--muted);font-size:.8rem}.chip{font-family:var(--mono);font-size:.72rem;background:var(--s3);border:1px solid var(--border2);padding:2px 8px;border-radius:6px}.ok{color:var(--ok);font-weight:700}.warn{color:var(--warn);font-weight:700}.bad{color:var(--bad);font-weight:700}.lat-bar{height:2px;background:var(--border);border-radius:2px;margin-top:1rem;overflow:hidden}.lat-fill{height:100%;border-radius:2px;transition:width .6s}.actions{display:flex;gap:.75rem;flex-wrap:wrap;margin-bottom:1.5rem}.btn{display:inline-flex;align-items:center;gap:.4rem;padding:.55rem 1.1rem;border-radius:9px;font-weight:700;font-size:.8rem;text-decoration:none;transition:all .2s;border:1px solid transparent}.btn-primary{background:linear-gradient(135deg,var(--accent),var(--accent2));color:#fff;box-shadow:0 4px 16px rgba(99,102,241,.2)}.btn-primary:hover{transform:translateY(-2px)}.btn-ghost{border-color:var(--border);color:var(--text);background:var(--s1)}.btn-ghost:hover{border-color:var(--accent);color:var(--accent)}.section{margin-bottom:1.5rem}.section-hdr{display:flex;align-items:center;gap:.5rem;margin-bottom:.75rem;font-family:var(--mono);font-size:.68rem;text-transform:uppercase;letter-spacing:1.5px;color:var(--muted)}.count{background:var(--s3);border:1px solid var(--border2);border-radius:8px;padding:0 7px;font-size:.6rem}.table-card{background:var(--s1);border:1px solid var(--border);border-radius:14px;overflow:hidden}table{width:100%;border-collapse:collapse;font-size:.82rem}thead tr{background:var(--s2)}th{padding:.65rem 1rem;text-align:left;font-family:var(--mono);font-size:.62rem;text-transform:uppercase;letter-spacing:1px;color:var(--muted);border-bottom:1px solid var(--border)}td{padding:.6rem 1rem;border-top:1px solid rgba(255,255,255,.03)}tbody tr:hover td{background:rgba(255,255,255,.02)}code{font-family:var(--mono);font-size:.73rem;color:var(--accent3);background:rgba(6,182,212,.08);padding:1px 6px;border-radius:4px}.pill{font-size:.65rem;font-family:var(--mono);color:var(--muted)}.pos-top{background:rgba(99,102,241,.15);border:1px solid rgba(99,102,241,.3);color:var(--accent);font-size:.6rem;font-family:var(--mono);padding:0 5px;border-radius:4px;font-weight:700}.dim{color:var(--dim)}.empty{text-align:center;color:var(--muted);padding:2rem;font-size:.8rem;font-family:var(--mono)}footer{text-align:center;color:var(--muted);font-size:.68rem;font-family:var(--mono);padding-top:2rem;border-top:1px solid var(--border);margin-top:1rem}"

    html = (
        "<!doctype html><html lang='en'><head>"
        "<meta charset='utf-8'/><meta name='viewport' content='width=device-width,initial-scale=1'/>"
        "<title>SCFiles Dashboard</title>"
        "<link href='https://fonts.googleapis.com/css2?family=JetBrains+Mono:wght@400;600;700&family=Syne:wght@400;600;700;800&display=swap' rel='stylesheet'>"
        f"<style>{css}</style></head><body>"
        "<div class='blob blob-1'></div><div class='blob blob-2'></div>"
        f"<header><div class='brand'><div class='logo-icon'>🎛</div>"
        f"<div><div class='brand-name'>SC<span>Files</span></div><div class='brand-sub'>backend manager</div></div></div>"
        f"<div style='display:flex;align-items:center;gap:1rem'>"
        f"<div class='live-pill'><div class='live-dot'></div>LIVE</div>"
        f"<span style='font-family:var(--mono);font-size:.68rem;color:var(--muted)'>{now.strftime('%H:%M:%S IST')}</span>"
        f"</div></header><div class='wrap'>"
        f"<div class='kpi-grid'>"
        f"<div class='kpi'><div class='kpi-lbl'>Movies</div><div class='kpi-val' id='kv0'>{len(movies)}</div><div class='kpi-sub'>{total_dls} download links</div></div>"
        f"<div class='kpi'><div class='kpi-lbl'>Series</div><div class='kpi-val' id='kv1'>{len(series)}</div><div class='kpi-sub'>{total_eps} total episodes</div></div>"
        f"<div class='kpi'><div class='kpi-lbl'>Collections</div><div class='kpi-val' id='kv2'>{len(cols)}</div><div class='kpi-sub'>curated playlists</div></div>"
        f"<div class='kpi'><div class='kpi-lbl'>Backend Latency</div>"
        f"<div class='kpi-val' id='kv3' style='font-size:1.5rem'>{b_ms:.0f}<span style='font-size:.9rem;-webkit-text-fill-color:var(--muted)'>ms</span></div>"
        f"<div class='kpi-sub'>last ping</div></div></div>"
        f"<div class='panels'>"
        f"<div class='panel'><div class='panel-hdr'>🤖 Bot Health</div>"
        f"<div class='srow'><span class='lbl'>Status</span><span class='ok'>● ONLINE</span></div>"
        f"<div class='srow'><span class='lbl'>Uptime</span><span class='chip'>{uptime}</span></div>"
        f"<div class='srow'><span class='lbl'>Last Backup</span><span class='chip'>{bk}</span></div>"
        f"<div class='srow'><span class='lbl'>Last Ping</span><span class='chip'>{pg}</span></div>"
        f"<div class='srow'><span class='lbl'>Backup Chat</span><span class='chip'>{state.BACKUP_TARGET or '—'}</span></div></div>"
        f"<div class='panel'><div class='panel-hdr'>🌐 Backend Health</div>"
        f"<div class='srow'><span class='lbl'>Status</span><span class='{s_cls}'>● {b_status.upper()}</span></div>"
        f"<div class='srow'><span class='lbl'>HTTP Code</span><span class='chip'>{b_code or 'N/A'}</span></div>"
        f"<div class='srow'><span class='lbl'>Latency</span><span class='chip'>{b_ms:.0f}ms</span></div>"
        f"<div class='srow'><span class='lbl'>URL</span><span class='chip' style='max-width:160px;overflow:hidden;text-overflow:ellipsis;white-space:nowrap'>{BACKEND_URL}</span></div>"
        f"{err_row}<div class='lat-bar'><div class='lat-fill' style='width:{lat_w}%;background:{lat_c}'></div></div></div></div>"
        f"<div class='actions'>"
        f"<a class='btn btn-primary' href='{admin_url}'>🎛 Admin Panel</a>"
        f"<a class='btn btn-ghost' href='/backup/all'>📦 Backup ZIP</a>"
        f"<a class='btn btn-ghost' href='/logs'>📋 Logs</a>"
        f"<a class='btn btn-ghost' href='/health'>📡 JSON</a>"
        f"<a class='btn btn-ghost' href='javascript:location.reload()'>🔄 Refresh</a></div>"
        f"<div class='section'><div class='section-hdr'>🎬 Recent Movies <span class='count'>{len(movies)}</span></div>"
        f"<div class='table-card'><table><thead><tr><th>ID</th><th>TMDB</th><th>Extras</th><th>Downloads</th><th>Pos</th></tr></thead>"
        f"<tbody>{mv_rows or empty}</tbody></table></div></div>"
        f"<div class='section'><div class='section-hdr'>📺 Recent Series <span class='count'>{len(series)}</span></div>"
        f"<div class='table-card'><table><thead><tr><th>ID</th><th>TMDB</th><th>Seasons</th><th>Episodes</th></tr></thead>"
        f"<tbody>{sr_rows or empty}</tbody></table></div></div>"
        f"<div class='section'><div class='section-hdr'>🗂 Collections <span class='count'>{len(cols)}</span></div>"
        f"<div class='table-card'><table><thead><tr><th>ID</th><th>Name</th><th>Movies</th></tr></thead>"
        f"<tbody>{co_rows or empty}</tbody></table></div></div>"
        f"<footer>SCFiles Dashboard · Auto-refresh 60s · {now.strftime('%d %b %Y, %H:%M:%S IST')}</footer>"
        f"</div>"
        "<script>setTimeout(()=>location.reload(),60000);"
        "document.querySelectorAll('[id^=kv]').forEach(el=>{const t=parseInt(el.textContent);if(isNaN(t)||t>9999)return;let c=0;const s=t/30;const ti=setInterval(()=>{c=Math.min(c+s,t);el.textContent=Math.floor(c);if(c>=t)clearInterval(ti);},30);});"
        "</script></body></html>"
    )
    return web.Response(text=html, content_type="text/html")

async def web_health(req: web.Request) -> web.Response:
    now = datetime.now(IST)
    async def ping():
        try:
            s=await sess(); t0=datetime.now()
            async with s.get(BACKEND_URL, timeout=aiohttp.ClientTimeout(total=10)) as r:
                return {"status":"online" if r.status==200 else "degraded",
                        "http_status":r.status,
                        "latency_ms":round((datetime.now()-t0).total_seconds()*1000,2),"error":None}
        except Exception as e: return {"status":"offline","http_status":None,"latency_ms":None,"error":str(e)}
    backend, (mv,sr,co) = await asyncio.gather(ping(), fetch_all())
    return web.json_response({
        "bot":{"status":"online","uptime_seconds":int((now-state.BOT_STARTED_AT).total_seconds()),
               "last_backup_at":state.LAST_BACKUP_AT.isoformat() if state.LAST_BACKUP_AT else None},
        "backend":backend,
        "db":{"movies":len(mv),"series":len(sr),"collections":len(co)},
        "time":now.isoformat()})

async def web_backup_zip(req: web.Request) -> web.Response:
    data,ts = await make_zip()
    return web.Response(body=data,headers={"Content-Type":"application/zip",
        "Content-Disposition":f'attachment; filename="backup_all_{ts}.zip"'})

async def web_logs(req: web.Request) -> web.Response:
    if not os.path.exists(LOG_FILE):
        return web.Response(text="No log file yet.",content_type="text/plain")
    with open(LOG_FILE,"rb") as f:
        f.seek(0,2); size=f.tell(); f.seek(max(0,size-65536)); tail=f.read()
    return web.Response(body=tail,headers={"Content-Type":"text/plain; charset=utf-8",
        "Content-Disposition":"inline; filename=bot.log"})

async def web_admin_logs(req: web.Request) -> web.Response:
    """Token-protected log tail for the admin panel."""
    if (err := _token_required(req)): return err
    return await web_logs(req)
