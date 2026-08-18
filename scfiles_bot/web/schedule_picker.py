"""
web/schedule_picker.py - the Schedule Notification Web App page, PLUS the
two JSON endpoints it talks to directly (GET /schedule/info, POST
/schedule/submit). This deliberately does NOT rely on Telegram's
Web App sendData()/web_app_data message delivery - the page just makes
plain fetch() calls to our own server, which is fully within our control
and doesn't depend on how a given Telegram client handles an inline
keyboard's Web App button. See handlers/notify_flow.py's module docstring
for the full rationale.

Everything the schedule flow needs (date/time, category, title) is
collected on ONE page and submitted in ONE request, so there's no
multi-step conversation to keep alive on the bot side at all - by the
time this page opens, the bot has already ended its conversation and the
admin is free to keep working.
"""
from datetime import datetime

import pytz
from aiohttp import web

import db
import notify
import notify_tokens
from config import IST, logger

_PAGE = """<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8"/>
<meta name="viewport" content="width=device-width,initial-scale=1,user-scalable=no"/>
<title>Schedule Notification</title>
<script src="https://telegram.org/js/telegram-web-app.js"></script>
<style>
  :root{
    color-scheme: dark;
    --bg-0:#05060a; --bg-1:#0b0d16;
    --accent:#7c5cff; --accent-2:#00e0c6; --accent-grad: linear-gradient(135deg, #7c5cff 0%, #00c2ff 50%, #00e0c6 100%);
    --text:#eef0ff; --muted:#8890b5; --danger:#ff5c7a; --success:#33e6a0;
    --border: rgba(255,255,255,.08); --glass: rgba(255,255,255,.045);
  }
  *{box-sizing:border-box; -webkit-tap-highlight-color: transparent;}
  html,body{height:100%;}
  body{
    margin:0; padding:22px 18px 28px; min-height:100vh;
    background:
      radial-gradient(1200px 600px at 15% -10%, rgba(124,92,255,.25), transparent 60%),
      radial-gradient(1000px 500px at 110% 10%, rgba(0,224,198,.16), transparent 55%),
      var(--bg-0);
    color: var(--text);
    font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif;
    display:flex; flex-direction:column; gap:18px;
    overflow-x:hidden;
  }
  .halo{
    position:fixed; pointer-events:none; z-index:0;
    width:280px; height:280px; border-radius:50%;
    background: var(--accent-grad); filter: blur(90px); opacity:.18;
    top:-80px; right:-80px;
  }
  header{position:relative; z-index:1;}
  .eyebrow{
    display:inline-flex; align-items:center; gap:6px; font-size:.68rem; font-weight:700;
    letter-spacing:.12em; text-transform:uppercase; color: var(--accent-2);
    background: rgba(0,224,198,.08); border:1px solid rgba(0,224,198,.25);
    padding:5px 10px; border-radius:999px; margin-bottom:10px;
  }
  .eyebrow .dot{width:6px; height:6px; border-radius:50%; background: var(--accent-2); box-shadow: 0 0 8px var(--accent-2); animation: pulse 1.6s infinite;}
  @keyframes pulse{0%,100%{opacity:1} 50%{opacity:.35}}
  h1{font-size:1.4rem; margin:0 0 6px; font-weight:800; letter-spacing:-.01em;
     background: var(--accent-grad); -webkit-background-clip:text; background-clip:text; color:transparent;}
  .hint{font-size:.85rem; color: var(--muted); line-height:1.55; max-width:34em;}

  .card{
    position:relative; z-index:1;
    background: var(--glass); border:1px solid var(--border); border-radius:18px;
    padding:18px; backdrop-filter: blur(18px); -webkit-backdrop-filter: blur(18px);
    box-shadow: 0 8px 30px rgba(0,0,0,.35);
  }
  label.field-label{
    display:block; font-size:.7rem; font-weight:700; text-transform:uppercase;
    letter-spacing:.08em; color: var(--muted); margin-bottom:10px;
  }

  input[type="datetime-local"], input[type="text"]{
    width:100%; padding:14px 14px; border-radius:13px; border:1px solid var(--border);
    background: rgba(255,255,255,.03); color: var(--text); font-size:1rem;
    outline:none; transition: border-color .18s ease, box-shadow .18s ease;
  }
  input[type="datetime-local"]:focus, input[type="text"]:focus{
    border-color: var(--accent); box-shadow: 0 0 0 3px rgba(124,92,255,.18);
  }
  input::-webkit-calendar-picker-indicator{filter: invert(1); opacity:.8;}

  .quick{display:flex; gap:8px; flex-wrap:wrap; margin-top:12px;}
  .quick button{
    flex:1; min-width:76px; padding:10px 8px; border-radius:11px; border:1px solid var(--border);
    background: rgba(255,255,255,.03); color: var(--muted); font-size:.8rem; font-weight:600; cursor:pointer;
    transition: all .15s ease;
  }
  .quick button:active{transform: scale(.96); background: rgba(124,92,255,.15); color:var(--text); border-color: var(--accent);}

  .pills{display:flex; gap:8px; margin-top:2px;}
  .pill{
    flex:1; text-align:center; padding:12px 8px; border-radius:13px; border:1px solid var(--border);
    background: rgba(255,255,255,.03); font-size:.82rem; font-weight:700; cursor:pointer;
    color: var(--muted); transition: all .18s ease; user-select:none;
  }
  .pill.active{
    color:#050609; border-color: transparent;
    background: var(--accent-grad); box-shadow: 0 4px 18px rgba(124,92,255,.4);
  }

  #err{color: var(--danger); font-size:.82rem; min-height:1.2em; text-align:center; position:relative; z-index:1;}

  #confirmBtn{
    position:relative; z-index:1; margin-top:auto; padding:17px; border:none; border-radius:15px;
    font-size:1.02rem; font-weight:800; letter-spacing:.01em;
    background: var(--accent-grad); color:#05060a; cursor:pointer;
    box-shadow: 0 10px 30px rgba(124,92,255,.35);
    transition: transform .12s ease, box-shadow .12s ease, opacity .2s ease;
  }
  #confirmBtn:active{transform: scale(.98);}
  #confirmBtn:disabled{opacity:.5; cursor:not-allowed; box-shadow:none;}

  .success{
    display:none; flex-direction:column; align-items:center; gap:14px; text-align:center;
    padding: 40px 10px; z-index:1; position:relative;
  }
  .success.show{display:flex;}
  .success .ring{
    width:64px; height:64px; border-radius:50%; display:flex; align-items:center; justify-content:center;
    background: rgba(51,230,160,.12); border:1px solid rgba(51,230,160,.4);
    font-size:1.8rem; color: var(--success); animation: pop .35s ease;
  }
  @keyframes pop{from{transform:scale(.5); opacity:0} to{transform:scale(1); opacity:1}}
  .success h2{margin:0; font-size:1.1rem;}
  .success p{margin:0; color: var(--muted); font-size:.85rem;}

  #formArea{display:flex; flex-direction:column; gap:14px;}
</style>
</head>
<body>
  <div class="halo"></div>

  <header>
    <div class="eyebrow"><span class="dot"></span> Schedule Notification</div>
    <h1>Pick when this goes out</h1>
    <div class="hint">Set a date, time (IST), category, and title - this will
    post automatically once, with no further steps needed.</div>
  </header>

  <div id="formArea">
    <div class="card">
      <label class="field-label">Date &amp; time (IST)</label>
      <input type="datetime-local" id="dt"/>
      <div class="quick">
        <button type="button" data-mins="60">+1h</button>
        <button type="button" data-mins="180">+3h</button>
        <button type="button" data-mins="1440">+1 day</button>
        <button type="button" data-mins="10080">+1 week</button>
      </div>
    </div>

    <div class="card">
      <label class="field-label">Category</label>
      <div class="pills" id="catPills">
        <div class="pill active" data-cat="hd">HD</div>
        <div class="pill" data-cat="predvd">PreDVD</div>
        <div class="pill" data-cat="all">All</div>
      </div>
    </div>

    <div class="card">
      <label class="field-label">Title</label>
      <input type="text" id="title" placeholder="Loading suggestion..."/>
    </div>

    <div id="err"></div>
    <button id="confirmBtn">Confirm Schedule</button>
  </div>

  <div class="success" id="successBox">
    <div class="ring">OK</div>
    <h2>Scheduled!</h2>
    <p id="successDetail">Closing...</p>
  </div>

<script>
  var tg = window.Telegram && window.Telegram.WebApp;
  if (tg) { tg.ready(); tg.expand(); }

  function getToken() {
    var params = new URLSearchParams(window.location.search);
    return params.get('token') || '';
  }
  var TOKEN = getToken();

  function nowIST() {
    var now = new Date();
    var istOffsetMin = 330;
    return new Date(now.getTime() + (istOffsetMin + now.getTimezoneOffset()) * 60000);
  }
  function toInputValue(d) {
    function pad(n){ return String(n).padStart(2, '0'); }
    return d.getFullYear() + '-' + pad(d.getMonth()+1) + '-' + pad(d.getDate()) + 'T' + pad(d.getHours()) + ':' + pad(d.getMinutes());
  }

  var dt = document.getElementById('dt');
  var err = document.getElementById('err');
  var confirmBtn = document.getElementById('confirmBtn');
  var titleInput = document.getElementById('title');
  var catPills = document.querySelectorAll('.pill');
  var selectedCat = 'hd';

  var base = nowIST();
  dt.min = toInputValue(base);
  dt.value = toInputValue(new Date(base.getTime() + 60 * 60000));

  document.querySelectorAll('.quick button').forEach(function(btn){
    btn.addEventListener('click', function(){
      var mins = parseInt(btn.dataset.mins, 10);
      dt.value = toInputValue(new Date(base.getTime() + mins * 60000));
    });
  });

  catPills.forEach(function(pill){
    pill.addEventListener('click', function(){
      catPills.forEach(function(p){ p.classList.remove('active'); });
      pill.classList.add('active');
      selectedCat = pill.dataset.cat;
    });
  });

  fetch('/schedule/info?token=' + encodeURIComponent(TOKEN))
    .then(function(r){ return r.json(); })
    .then(function(data){
      if (data.ok) {
        titleInput.value = data.title_suggestion || '';
        titleInput.placeholder = 'Notification title';
      } else {
        err.textContent = data.error || 'This link has expired - go back and try again.';
        confirmBtn.disabled = true;
      }
    })
    .catch(function(){
      titleInput.placeholder = 'Notification title';
    });

  confirmBtn.addEventListener('click', function() {
    err.textContent = '';
    if (!dt.value) { err.textContent = 'Pick a date and time first.'; return; }
    var nowVal = toInputValue(base);
    if (dt.value < nowVal) { err.textContent = 'That time is in the past - pick a later time.'; return; }
    if (!titleInput.value.trim()) { err.textContent = 'Enter a title.'; return; }

    confirmBtn.disabled = true;
    confirmBtn.textContent = 'Scheduling...';

    fetch('/schedule/submit', {
      method: 'POST',
      headers: {'Content-Type': 'application/json'},
      body: JSON.stringify({
        token: TOKEN,
        scheduled_at_ist: dt.value,
        category: selectedCat,
        title: titleInput.value.trim(),
      }),
    })
      .then(function(r){ return r.json(); })
      .then(function(data){
        if (!data.ok) {
          err.textContent = data.error || 'Something went wrong - try again.';
          confirmBtn.disabled = false;
          confirmBtn.textContent = 'Confirm Schedule';
          return;
        }
        document.getElementById('formArea').style.display = 'none';
        document.getElementById('successBox').classList.add('show');
        document.getElementById('successDetail').textContent = data.display_time
          ? ('Scheduled for ' + data.display_time) : 'Closing...';
        setTimeout(function(){
          if (tg) { tg.close(); }
        }, 1400);
      })
      .catch(function(){
        err.textContent = 'Network error - try again.';
        confirmBtn.disabled = false;
        confirmBtn.textContent = 'Confirm Schedule';
      });
  });
</script>
</body>
</html>
"""


async def web_schedule_picker(req: web.Request) -> web.Response:
    return web.Response(text=_PAGE, content_type="text/html")


async def web_schedule_info(req: web.Request) -> web.Response:
    token = req.query.get("token", "")
    entry = notify_tokens.get(token)
    if not entry:
        return web.json_response({"ok": False, "error": "This link has expired or was already used."})
    item = entry.get("item", {})
    suggestion = item.get("title") or item.get("id", "")
    return web.json_response({"ok": True, "title_suggestion": suggestion, "kind": entry.get("kind", "")})


async def web_schedule_submit(req: web.Request) -> web.Response:
    try:
        body = await req.json()
    except Exception:
        return web.json_response({"ok": False, "error": "Malformed request."}, status=400)

    token = body.get("token", "")
    entry = notify_tokens.get(token)
    if not entry:
        return web.json_response({"ok": False, "error": "This link has expired or was already used."})

    category = str(body.get("category", "hd")).lower()
    if category not in notify.CATEGORIES:
        category = "hd"
    title = (body.get("title") or "").strip()
    if not title:
        return web.json_response({"ok": False, "error": "Title is required."})

    try:
        picked = body["scheduled_at_ist"]
        naive_ist = datetime.strptime(picked, "%Y-%m-%dT%H:%M")
        scheduled_ist = IST.localize(naive_ist)
    except Exception:
        return web.json_response({"ok": False, "error": "Invalid date/time."})

    if scheduled_ist < datetime.now(IST):
        return web.json_response({"ok": False, "error": "That time is in the past."})

    scheduled_utc_naive = scheduled_ist.astimezone(pytz.utc).replace(tzinfo=None)
    display_time = scheduled_ist.strftime("%d %b %Y, %H:%M IST")

    notify_tokens.pop(token)  # one-shot - can't be submitted twice

    doc = {
        "kind": entry["kind"], "item": entry["item"], "poster_url": entry.get("poster_url"),
        "category": category, "title": title,
        "scheduled_at": scheduled_utc_naive.isoformat(),
        "scheduled_at_display": display_time,
        "created_by": None,
    }
    sched_id = await db.add_scheduled_notification(doc)

    admin_bot = req.app.get("admin_bot")
    chat_id = entry.get("chat_id")

    # Delete the original "Yes, notify / Yes, schedule / No, skip" prompt —
    # tapping the Web App button produces no bot-side event, so this is the
    # only point where we can clean it up: right when scheduling actually
    # completes. Best-effort; a failure here (message already deleted,
    # too old, etc) shouldn't block the rest of the response.
    prompt_chat_id = entry.get("prompt_chat_id")
    prompt_message_id = entry.get("prompt_message_id")
    if admin_bot and prompt_chat_id and prompt_message_id:
        try:
            await admin_bot.delete_message(prompt_chat_id, prompt_message_id)
        except Exception as e:
            logger.warning("Couldn't delete the original notify prompt %s/%s: %s",
                           prompt_chat_id, prompt_message_id, e)

    if admin_bot and chat_id:
        try:
            msg = await admin_bot.send_message(
                chat_id,
                f"🗓 <b>\"{title}\"</b> has been scheduled for <b>{display_time}</b>.",
                parse_mode="HTML",
            )
            await db.set_scheduled_confirmation_message(sched_id, chat_id, msg.message_id)
        except Exception as e:
            logger.warning("Couldn't send schedule confirmation message: %s", e)

    return web.json_response({"ok": True, "display_time": display_time})
