"""
web/schedule_picker.py — a minimal page that opens as a Telegram "Web App"
button (see handlers/notify_flow.py's schedule button). It uses Telegram's
WebApp JS bridge to send the picked date/time back to the bot as a normal
message (a web_app_data update) and then closes itself - no server-side
state, no auth needed here: the page is inert without being opened via an
actual Telegram Web App button, and performs no backend calls of its own.

Time is IST (Asia/Kolkata) - the same timezone the rest of the bot uses
for backups/logs - labelled clearly since an HTML datetime-local input has
no timezone of its own; the picked value is treated as IST wall-clock time.
"""
from aiohttp import web

_PAGE = """<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8"/>
<meta name="viewport" content="width=device-width,initial-scale=1,user-scalable=no"/>
<title>Schedule Notification</title>
<script src="https://telegram.org/js/telegram-web-app.js"></script>
<style>
  :root{color-scheme: dark;}
  *{box-sizing:border-box;}
  body{
    margin:0; padding:24px 20px; min-height:100vh;
    background: var(--tg-theme-bg-color, #0f1115);
    color: var(--tg-theme-text-color, #f0f0f0);
    font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif;
    display:flex; flex-direction:column; gap:20px;
  }
  h1{font-size:1.15rem; margin:0 0 4px;}
  .hint{font-size:.82rem; color: var(--tg-theme-hint-color, #8a8a8a); line-height:1.5;}
  label{font-size:.78rem; text-transform:uppercase; letter-spacing:.06em; color: var(--tg-theme-hint-color, #8a8a8a);}
  input[type="datetime-local"]{
    width:100%; padding:14px; margin-top:8px; border-radius:12px; border:1px solid #333;
    background: var(--tg-theme-secondary-bg-color, #1c1f26);
    color: var(--tg-theme-text-color, #f0f0f0);
    font-size:1rem;
  }
  .quick{display:flex; gap:8px; flex-wrap:wrap; margin-top:4px;}
  .quick button{
    flex:1; min-width:80px; padding:10px; border-radius:10px; border:1px solid #333;
    background: var(--tg-theme-secondary-bg-color, #1c1f26); color:inherit; font-size:.85rem; cursor:pointer;
  }
  .quick button:active{opacity:.7;}
  #confirmBtn{
    margin-top:auto; padding:16px; border:none; border-radius:12px; font-size:1rem; font-weight:600;
    background: var(--tg-theme-button-color, #2ea6ff); color: var(--tg-theme-button-text-color, #ffffff);
    cursor:pointer;
  }
  #confirmBtn:disabled{opacity:.4; cursor:not-allowed;}
  #err{color:#ff6b6b; font-size:.82rem; min-height:1.2em;}
</style>
</head>
<body>
  <div>
    <h1>Schedule Notification</h1>
    <div class="hint">Pick a date and time (India Standard Time, IST) - the
    notification will send automatically then.</div>
  </div>

  <div>
    <label for="dt">Date and time (IST)</label>
    <input type="datetime-local" id="dt"/>
    <div class="quick">
      <button type="button" data-mins="60">+1h</button>
      <button type="button" data-mins="180">+3h</button>
      <button type="button" data-mins="1440">+1 day</button>
      <button type="button" data-mins="10080">+1 week</button>
    </div>
  </div>

  <div id="err"></div>
  <button id="confirmBtn">Confirm Schedule</button>

<script>
  var tg = window.Telegram && window.Telegram.WebApp;
  if (tg) { tg.ready(); tg.expand(); }

  function nowIST() {
    var now = new Date();
    var istOffsetMin = 330;
    var ist = new Date(now.getTime() + (istOffsetMin + now.getTimezoneOffset()) * 60000);
    return ist;
  }
  function toInputValue(d) {
    function pad(n){ return String(n).padStart(2, '0'); }
    return d.getFullYear() + '-' + pad(d.getMonth()+1) + '-' + pad(d.getDate()) + 'T' + pad(d.getHours()) + ':' + pad(d.getMinutes());
  }

  var dt = document.getElementById('dt');
  var err = document.getElementById('err');
  var confirmBtn = document.getElementById('confirmBtn');

  var base = nowIST();
  dt.min = toInputValue(base);
  var defaultTime = new Date(base.getTime() + 60 * 60000);
  dt.value = toInputValue(defaultTime);

  var quickButtons = document.querySelectorAll('.quick button');
  for (var i = 0; i < quickButtons.length; i++) {
    quickButtons[i].addEventListener('click', function(ev) {
      var mins = parseInt(ev.target.dataset.mins, 10);
      dt.value = toInputValue(new Date(base.getTime() + mins * 60000));
    });
  }

  confirmBtn.addEventListener('click', function() {
    err.textContent = '';
    if (!dt.value) { err.textContent = 'Pick a date and time first.'; return; }
    var picked = dt.value;
    var nowVal = toInputValue(base);
    if (picked < nowVal) { err.textContent = 'That time is in the past - pick a later time.'; return; }

    confirmBtn.disabled = true;
    confirmBtn.textContent = 'Scheduling...';
    var payload = JSON.stringify({ scheduled_at_ist: picked });
    if (tg) {
      tg.sendData(payload);
      tg.close();
    } else {
      err.textContent = "Open this page via the bot's Schedule button.";
      confirmBtn.disabled = false;
      confirmBtn.textContent = 'Confirm Schedule';
    }
  });
</script>
</body>
</html>
"""


async def web_schedule_picker(req: web.Request) -> web.Response:
    return web.Response(text=_PAGE, content_type="text/html")
