"""web/admin_panel.py — the token-protected /admin single-page app for
editing movies/series/collections directly in the browser. All markup and
JS live in _ADMIN_HTML; nothing here talks to the backend except by
injecting BACKEND_URL into the page so the browser can call the API
directly."""
from aiohttp import web

from config import BACKEND_URL
from web.dashboard import _token_required

# ═══════════════════════════════════════════════════════════════════════════════
_ADMIN_HTML = """\
<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8"><meta name="viewport" content="width=device-width,initial-scale=1.0">
<title>SC ADMIN</title>
<link href="https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700;800&display=swap" rel="stylesheet">
<link href="https://fonts.googleapis.com/css2?family=JetBrains+Mono:wght@400;500&display=swap" rel="stylesheet">
<style>
:root{--accent:#00d4aa;--accent-glow:rgba(0,212,170,.25);--accent-dim:rgba(0,212,170,.1);--accent-hover:#00e6b8;--bg:#09090b;--sidebar:#121214;--card:#18181b;--card-hover:#212126;--card-border:#27272a;--text:#f4f4f5;--text-muted:#a1a1aa;--text-dim:#71717a;--success:#22c55e;--danger:#ef4444;--warning:#f59e0b;--border:#27272a;}
*{box-sizing:border-box;margin:0;padding:0}
body{font-family:'Inter',sans-serif;background:var(--bg);color:var(--text);min-height:100vh;display:flex;overflow-x:hidden}
::-webkit-scrollbar{width:8px;height:8px}::-webkit-scrollbar-track{background:var(--bg)}::-webkit-scrollbar-thumb{background:#3f3f46;border-radius:4px}::-webkit-scrollbar-thumb:hover{background:#52525b}
.sidebar{width:260px;min-height:100vh;background:var(--sidebar);border-right:1px solid var(--border);display:flex;flex-direction:column;position:fixed;left:0;top:0;z-index:100;transition:transform .3s cubic-bezier(.4,0,.2,1)}
.sidebar-header{padding:24px;border-bottom:1px solid var(--border)}
.logo{font-size:1.25rem;font-weight:800;color:var(--accent);letter-spacing:-.5px;display:flex;align-items:center;gap:8px}
.nav-section{padding:24px 16px;flex:1}
.nav-label{font-size:.7rem;font-weight:700;color:var(--text-dim);text-transform:uppercase;letter-spacing:1.5px;padding:0 12px;margin-bottom:12px}
.nav-item{display:flex;align-items:center;gap:12px;padding:12px 16px;border-radius:8px;color:var(--text-muted);cursor:pointer;transition:all .2s;border:none;background:transparent;width:100%;text-align:left;font-size:.9rem;font-weight:500;margin-bottom:4px}
.nav-item:hover{background:rgba(255,255,255,.03);color:var(--text)}
.nav-item.active{background:var(--accent-dim);color:var(--accent);font-weight:600}
.nav-item svg{width:18px;height:18px;flex-shrink:0}
.stats-section{padding:20px 16px;border-top:1px solid var(--border)}
.stats-grid{display:grid;grid-template-columns:repeat(3,1fr);gap:10px}
.stat-card{background:var(--bg);border:1px solid var(--card-border);border-radius:8px;padding:12px 8px;text-align:center}
.stat-value{font-size:1.1rem;font-weight:700;color:var(--text)}
.stat-label{font-size:.65rem;color:var(--text-dim);text-transform:uppercase;margin-top:4px;font-weight:600;letter-spacing:.5px}
.log-section{padding:16px;border-top:1px solid var(--border);max-height:220px;display:flex;flex-direction:column}
.log-header{display:flex;justify-content:space-between;align-items:center;margin-bottom:12px}
.log-title{font-size:.7rem;font-weight:700;color:var(--text-dim);text-transform:uppercase;letter-spacing:1px}
.log-clear{font-size:.7rem;color:var(--accent);cursor:pointer;background:none;border:none;font-weight:600}
.log-box{flex:1;background:#000;border:1px solid var(--border);border-radius:8px;padding:12px;overflow-y:auto;font-family:'JetBrains Mono',monospace;font-size:.7rem;line-height:1.6;max-height:120px}
.log-entry{color:var(--text-muted);margin-bottom:4px;word-break:break-all}
.log-entry.error{color:var(--danger)}.log-entry.success{color:var(--success)}
.log-time{color:var(--text-dim);margin-right:8px;font-size:.65rem}
.status-bar{padding:16px 20px;border-top:1px solid var(--border);display:flex;align-items:center;gap:10px}
.status-dot{width:8px;height:8px;border-radius:50%;background:var(--danger);transition:all .3s}
.status-dot.online{background:var(--accent);box-shadow:0 0 10px var(--accent)}
.status-text{font-size:.8rem;font-weight:500;color:var(--text-muted)}
.main{flex:1;margin-left:260px;padding:32px 40px;min-height:100vh;max-width:calc(100vw - 260px)}
.header{display:flex;justify-content:space-between;align-items:center;margin-bottom:32px;flex-wrap:wrap;gap:20px}
.header-left{display:flex;align-items:center;gap:16px}
.menu-toggle{display:none;background:var(--card);border:1px solid var(--border);color:var(--text);padding:10px;border-radius:8px;cursor:pointer}
.page-title{font-size:1.75rem;font-weight:800;letter-spacing:-.5px}
.header-actions{display:flex;gap:12px}
.btn{display:inline-flex;align-items:center;justify-content:center;gap:8px;padding:10px 20px;border-radius:8px;font-size:.875rem;font-weight:600;cursor:pointer;transition:all .2s;border:none;text-decoration:none}
.btn-primary{background:var(--accent);color:#000;box-shadow:0 4px 12px rgba(0,212,170,.2)}
.btn-primary:hover{background:var(--accent-hover);transform:translateY(-1px)}
.btn-secondary{background:var(--card);color:var(--text);border:1px solid var(--border)}
.btn-secondary:hover{border-color:var(--text-muted);background:var(--card-hover)}
.search-bar{margin-bottom:32px;max-width:600px}
.search-input{position:relative;width:100%}
.search-input input{width:100%;padding:14px 16px 14px 44px;background:var(--card);border:1px solid var(--border);border-radius:10px;color:var(--text);font-size:.95rem;outline:none;transition:all .2s}
.search-input input:focus{border-color:var(--accent);box-shadow:0 0 0 3px var(--accent-dim)}
.search-input input::placeholder{color:var(--text-dim)}
.search-input svg{position:absolute;left:16px;top:50%;transform:translateY(-50%);color:var(--text-dim);width:18px;height:18px}
.cards-grid{display:grid;grid-template-columns:repeat(auto-fill,minmax(300px,1fr));gap:20px}
.item-card{background:var(--card);border:1px solid var(--border);border-radius:12px;padding:20px;transition:all .2s;position:relative;display:flex;flex-direction:column;justify-content:space-between;min-height:120px}
.item-card:hover{border-color:var(--accent);transform:translateY(-4px);box-shadow:0 12px 30px rgba(0,0,0,.4)}
.card-header{display:flex;justify-content:space-between;align-items:flex-start;margin-bottom:16px}
.card-title{font-size:1rem;font-weight:600;color:var(--text);word-break:break-word;line-height:1.4;padding-right:70px}
.card-actions{position:absolute;top:16px;right:16px;display:flex;gap:8px}
.card-btn{background:var(--bg);border:1px solid var(--border);color:var(--text-muted);width:32px;height:32px;border-radius:6px;cursor:pointer;transition:all .2s;display:flex;align-items:center;justify-content:center}
.card-btn:hover{background:var(--accent-dim);color:var(--accent);border-color:var(--accent)}
.card-btn.delete:hover{background:rgba(239,68,68,.1);color:var(--danger);border-color:var(--danger)}
.card-meta{font-size:.8rem;color:var(--text-dim);display:flex;flex-direction:column;gap:4px;font-weight:500;border-top:1px solid var(--border);padding-top:12px}
.card-meta span{display:flex;align-items:center;gap:6px}
.modal-overlay{display:none;position:fixed;inset:0;background:rgba(0,0,0,.85);z-index:200;backdrop-filter:blur(4px);padding:20px;overflow-y:auto;align-items:flex-start;justify-content:center}
.modal-overlay.active{display:flex}
.modal{background:var(--card);border:1px solid var(--border);border-radius:16px;width:100%;max-width:950px;margin:40px auto;display:flex;flex-direction:column;box-shadow:0 25px 50px -12px rgba(0,0,0,.5)}
.modal-header{padding:20px 32px;border-bottom:1px solid var(--border);display:flex;justify-content:space-between;align-items:center;border-radius:16px 16px 0 0}
.modal-header h2{font-size:1.25rem;font-weight:700}
.modal-close{background:transparent;border:1px solid transparent;color:var(--text-muted);cursor:pointer;padding:6px;border-radius:6px;transition:all .2s}
.modal-close:hover{background:rgba(255,255,255,.05);color:var(--text)}
.modal-body{padding:32px;flex:1;display:flex;gap:40px}
.modal-footer{padding:20px 32px;border-top:1px solid var(--border);display:flex;justify-content:space-between;align-items:center;border-radius:0 0 16px 16px}
.form-area{flex:1;min-width:0}
.form-grid{display:grid;grid-template-columns:1fr 1fr;gap:20px;margin-bottom:24px}
.form-group{display:flex;flex-direction:column}
.form-group.full{grid-column:span 2}
.form-label{font-size:.75rem;font-weight:600;color:var(--text-muted);text-transform:uppercase;margin-bottom:8px;letter-spacing:.5px}
.form-input,.form-select{width:100%;padding:12px 16px;background:var(--bg);border:1px solid var(--border);border-radius:8px;color:var(--text);font-size:.9rem;outline:none;transition:all .2s;font-family:'Inter',sans-serif}
.form-input:focus,.form-select:focus{border-color:var(--accent);box-shadow:0 0 0 3px var(--accent-dim)}
.form-input::placeholder{color:var(--text-dim)}
.section-header{display:flex;justify-content:space-between;align-items:center;margin:32px 0 16px;padding-bottom:12px;border-bottom:1px solid var(--border)}
.section-title{font-size:.85rem;font-weight:700;color:var(--text);text-transform:uppercase;letter-spacing:1px}
.sub-row input,.ep-row input,.coll-row input,.season-header input{padding:10px 14px!important;background:var(--bg)!important;border:1px solid var(--border)!important;border-radius:6px!important;color:var(--text)!important;font-size:.85rem!important;outline:none!important;width:100%!important;margin:0!important}
.sub-row input:focus,.ep-row input:focus,.coll-row input:focus,.season-header input:focus{border-color:var(--accent)!important;box-shadow:0 0 0 2px var(--accent-dim)!important}
.sub-row{display:grid;grid-template-columns:120px 1fr 40px;gap:12px;margin-bottom:12px;align-items:center;background:rgba(255,255,255,.015);padding:12px;border-radius:8px;border:1px solid var(--border)}
.season-block{background:rgba(255,255,255,.01);border:1px solid var(--border);border-radius:12px;padding:20px;margin-bottom:24px}
.season-header{display:flex;justify-content:space-between;align-items:center;margin-bottom:16px;padding-bottom:16px;border-bottom:1px solid var(--border)}
.ep-row{display:grid;grid-template-columns:70px 1fr 1fr 1fr 1fr 40px;gap:10px;margin-bottom:12px;align-items:center}
.coll-row{display:grid;grid-template-columns:1.5fr 100px 100px 2fr 40px;gap:12px;margin-bottom:12px;align-items:center;background:rgba(255,255,255,.015);padding:12px;border-radius:8px;border:1px solid var(--border)}
.del-btn{background:rgba(239,68,68,.1)!important;color:var(--danger)!important;border:1px solid transparent!important;border-radius:6px!important;cursor:pointer;transition:all .2s!important;display:flex;align-items:center;justify-content:center;font-weight:800;font-size:.9rem;height:100%;min-height:38px}
.del-btn:hover{background:var(--danger)!important;color:#fff!important}
.preview-panel{width:240px;flex-shrink:0;display:flex;flex-direction:column;gap:16px}
.preview-box{width:100%;aspect-ratio:2/3;background:var(--bg);border-radius:12px;border:1px solid var(--border);display:flex;align-items:center;justify-content:center;overflow:hidden}
.preview-box img{width:100%;height:100%;object-fit:cover}
.preview-title{font-size:.9rem;font-weight:600;color:var(--text);text-align:center;line-height:1.4}
.toast{position:fixed;bottom:32px;right:32px;background:var(--card);border:1px solid var(--border);padding:16px 24px;border-radius:10px;font-size:.9rem;font-weight:500;z-index:9999;transform:translateY(150px);opacity:0;transition:all .4s cubic-bezier(.175,.885,.32,1.275)}
.toast.show{transform:translateY(0);opacity:1}
.toast.success{border-left:4px solid var(--success)}.toast.error{border-left:4px solid var(--danger)}
.confirm-overlay{display:none;position:fixed;inset:0;background:rgba(0,0,0,.8);z-index:300;align-items:center;justify-content:center;backdrop-filter:blur(4px)}
.confirm-overlay.active{display:flex}
.confirm-box{background:var(--card);border:1px solid var(--border);border-radius:12px;padding:32px;max-width:400px;width:90%;text-align:center}
.confirm-box h3{font-size:1.1rem;font-weight:700;margin-bottom:12px}
.confirm-box p{color:var(--text-muted);font-size:.9rem;margin-bottom:24px}
.confirm-actions{display:flex;gap:12px;justify-content:center}
.overlay{display:none;position:fixed;inset:0;background:rgba(0,0,0,.6);z-index:90;backdrop-filter:blur(2px)}
@media(max-width:1024px){.modal-body{flex-direction:column-reverse}.preview-panel{width:100%;max-width:240px;margin:0 auto}}
@media(max-width:768px){
  .sidebar{transform:translateX(-100%)}.sidebar.open{transform:translateX(0);box-shadow:20px 0 50px rgba(0,0,0,.5)}
  .main{margin-left:0;padding:24px 20px;max-width:100vw}.menu-toggle{display:block}
  .form-grid{grid-template-columns:1fr}.form-group.full{grid-column:span 1}
  .modal{margin:0;min-height:100vh;border-radius:0;border:none}.modal-overlay{padding:0}
  .sub-row,.ep-row,.coll-row{display:flex;flex-direction:column;align-items:stretch;padding:16px;border-radius:12px;gap:8px}
  .del-btn{min-height:44px;margin-top:8px;border-radius:8px!important}
  .season-header{flex-direction:column;gap:16px;align-items:flex-start}
}
</style>
</head>
<body>
<div class="overlay" id="overlay" onclick="toggleSidebar()"></div>
<aside class="sidebar" id="sidebar">
  <div class="sidebar-header">
    <div class="logo">
      <svg width="22" height="22" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2.5"><polygon points="12 2 15.09 8.26 22 9.27 17 14.14 18.18 21.02 12 17.77 5.82 21.02 7 14.14 2 9.27 8.91 8.26 12 2"/></svg>
      SC ADMIN
    </div>
  </div>
  <nav class="nav-section">
    <div class="nav-label">Content</div>
    <button class="nav-item active" onclick="switchView('movies')" id="nav-movies">
      <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><rect x="2" y="2" width="20" height="20" rx="2.18"/><line x1="7" y1="2" x2="7" y2="22"/><line x1="17" y1="2" x2="17" y2="22"/></svg>Movies
    </button>
    <button class="nav-item" onclick="switchView('series')" id="nav-series">
      <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><rect x="2" y="7" width="20" height="15" rx="2"/><polyline points="17 2 12 7 7 2"/></svg>Series
    </button>
    <button class="nav-item" onclick="switchView('collections')" id="nav-collections">
      <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><path d="M22 19a2 2 0 0 1-2 2H4a2 2 0 0 1-2-2V5a2 2 0 0 1 2-2h5l2 3h9a2 2 0 0 1 2 2z"/></svg>Collections
    </button>
  </nav>
  <div class="stats-section">
    <div class="stats-grid">
      <div class="stat-card"><div class="stat-value" id="statMovies">–</div><div class="stat-label">Movies</div></div>
      <div class="stat-card"><div class="stat-value" id="statSeries">–</div><div class="stat-label">Series</div></div>
      <div class="stat-card"><div class="stat-value" id="statColls">–</div><div class="stat-label">Colls</div></div>
    </div>
  </div>
  <div class="log-section">
    <div class="log-header"><span class="log-title">Activity Log</span><button class="log-clear" onclick="clearLogs()">Clear</button></div>
    <div class="log-box" id="logWindow"></div>
  </div>
  <div class="status-bar"><div class="status-dot" id="statusDot"></div><span class="status-text" id="statusText">Connecting...</span></div>
</aside>

<main class="main">
  <div class="header">
    <div class="header-left">
      <button class="menu-toggle" onclick="toggleSidebar()">
        <svg width="24" height="24" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><line x1="3" y1="12" x2="21" y2="12"/><line x1="3" y1="6" x2="21" y2="6"/><line x1="3" y1="18" x2="21" y2="18"/></svg>
      </button>
      <h1 class="page-title" id="pageTitle">Movies</h1>
    </div>
    <div class="header-actions">
      <button class="btn btn-secondary" onclick="downloadAll()">
        <svg width="18" height="18" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><path d="M21 15v4a2 2 0 0 1-2 2H5a2 2 0 0 1-2-2v-4"/><polyline points="7 10 12 15 17 10"/><line x1="12" y1="15" x2="12" y2="3"/></svg>Backup
      </button>
      <button class="btn btn-primary" onclick="openModal()">
        <svg width="18" height="18" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><line x1="12" y1="5" x2="12" y2="19"/><line x1="5" y1="12" x2="19" y2="12"/></svg>New Entry
      </button>
    </div>
  </div>
  <div class="search-bar">
    <div class="search-input">
      <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><circle cx="11" cy="11" r="8"/><line x1="21" y1="21" x2="16.65" y2="16.65"/></svg>
      <input type="text" id="searchInput" placeholder="Search by ID or name..." oninput="debounceSearch()">
    </div>
  </div>
  <div class="cards-grid" id="cardsGrid"></div>
</main>

<!-- Edit/Create modal -->
<div class="modal-overlay" id="modalOverlay">
  <div class="modal">
    <div class="modal-header">
      <h2 id="modalTitle">New Entry</h2>
      <button class="modal-close" onclick="closeModal()">
        <svg width="24" height="24" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><line x1="18" y1="6" x2="6" y2="18"/><line x1="6" y1="6" x2="18" y2="18"/></svg>
      </button>
    </div>
    <div class="modal-body">
      <div class="form-area">
        <form id="entryForm" onsubmit="return false;">
          <div class="form-grid">
            <div class="form-group"><label class="form-label">TMDB ID</label><input type="number" class="form-input" id="tmdbId" placeholder="e.g. 550" oninput="debounceAutoFetch(this.value)"></div>
            <div class="form-group"><label class="form-label">Slug ID</label><input type="text" class="form-input" id="mainId" placeholder="e.g. fight-club" required></div>
          </div>
          <!-- Movies fields -->
          <div id="moviesFields">
            <div class="section-header"><span class="section-title">Download Links</span></div>
            <div class="form-grid">
              <div class="form-group"><label class="form-label">480p URL</label><input type="text" class="form-input" id="m480" placeholder="https://..."></div>
              <div class="form-group"><label class="form-label">720p URL</label><input type="text" class="form-input" id="m720" placeholder="https://..."></div>
              <div class="form-group full"><label class="form-label">1080p URL</label><input type="text" class="form-input" id="m1080" placeholder="https://..."></div>
            </div>
            <div class="section-header"><span class="section-title">Subtitles</span><button type="button" class="btn btn-secondary" style="padding:8px 14px;font-size:.75rem;" onclick="addSubtitleRow()">+ Add</button></div>
            <div id="subtitleRows"></div>
            <div class="section-header"><span class="section-title">Metadata</span></div>
            <div class="form-grid">
              <div class="form-group"><label class="form-label">Extras</label><input type="text" class="form-input" id="mExtras" placeholder="e.g. Dubbed, HDRip"></div>
              <div class="form-group"><label class="form-label">Position</label><select class="form-select" id="mPos"><option value="top">Top</option><option value="bottom">Bottom</option></select></div>
            </div>
          </div>
          <!-- Series fields -->
          <div id="seriesFields" style="display:none;">
            <div class="section-header"><span class="section-title">Seasons &amp; Episodes</span><button type="button" class="btn btn-secondary" style="padding:8px 14px;font-size:.75rem;" onclick="addSeason()">+ Season</button></div>
            <div id="seasonsContainer"></div>
          </div>
          <!-- Collections fields -->
          <div id="collectionsFields" style="display:none;">
            <div class="form-grid">
              <div class="form-group full"><label class="form-label">Collection Name</label><input type="text" class="form-input" id="cName" placeholder="Enter title"></div>
              <div class="form-group"><label class="form-label">Banner URL</label><input type="text" class="form-input" id="cBanner" placeholder="https://..."></div>
              <div class="form-group"><label class="form-label">BG Music URL</label><input type="text" class="form-input" id="cBgm" placeholder="https://..."></div>
            </div>
            <div class="section-header"><span class="section-title">Movies</span><button type="button" class="btn btn-secondary" style="padding:8px 14px;font-size:.75rem;" onclick="addCollMovieRow()">+ Movie</button></div>
            <div id="collMoviesContainer"></div>
          </div>
        </form>
      </div>
      <div class="preview-panel">
        <div class="preview-box" id="previewBox">
          <svg width="48" height="48" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.5" style="color:var(--text-dim)"><rect x="3" y="3" width="18" height="18" rx="2"/><circle cx="8.5" cy="8.5" r="1.5"/><polyline points="21 15 16 10 5 21"/></svg>
        </div>
        <div class="preview-title" id="previewTitle">Image Preview</div>
      </div>
    </div>
    <div class="modal-footer">
      <button class="btn" style="background:rgba(239,68,68,.1);color:var(--danger);" id="deleteBtn" onclick="askDelete()">Delete</button>
      <div style="display:flex;gap:12px;margin-left:auto;">
        <button class="btn btn-secondary" onclick="closeModal()">Cancel</button>
        <button class="btn btn-primary" id="saveBtn" onclick="saveEntry()">Save</button>
      </div>
    </div>
  </div>
</div>

<!-- Custom confirm dialog -->
<div class="confirm-overlay" id="confirmOverlay">
  <div class="confirm-box">
    <h3 id="confirmTitle">Confirm</h3>
    <p id="confirmMsg">Are you sure?</p>
    <div class="confirm-actions">
      <button class="btn btn-secondary" onclick="closeConfirm()">Cancel</button>
      <button class="btn" style="background:var(--danger);color:#fff;" id="confirmOkBtn">Delete</button>
    </div>
  </div>
</div>

<div class="toast" id="toast"><span id="toastMsg"></span></div>

<script>
var API = '__BACKEND_URL__/api';
var TMDB_PROXY = window.location.origin + '/api/tmdb';
var view = 'movies', originalData = [], cachedData = [], isEdit = false, editingId = null;
var searchTimer = null, fetchTimer = null;

function toggleSidebar(){document.getElementById('sidebar').classList.toggle('open');document.getElementById('overlay').style.display=document.getElementById('sidebar').classList.contains('open')?'block':'none';}
function showToast(msg,type){var t=document.getElementById('toast');document.getElementById('toastMsg').textContent=msg;t.className='toast '+(type||'success')+' show';setTimeout(function(){t.classList.remove('show');},3200);}
function log(msg,type){var w=document.getElementById('logWindow');var tm=new Date().toLocaleTimeString('en-US',{hour12:false});w.innerHTML+='<div class="log-entry'+(type?' '+type:'')+'"><span class="log-time">['+tm+']</span>'+msg+'</div>';w.scrollTop=w.scrollHeight;}
function clearLogs(){document.getElementById('logWindow').innerHTML='';}

function showConfirm(title,msg,cb){
  document.getElementById('confirmTitle').textContent=title;
  document.getElementById('confirmMsg').textContent=msg;
  document.getElementById('confirmOkBtn').onclick=function(){closeConfirm();if(cb)cb();};
  document.getElementById('confirmOverlay').classList.add('active');
}
function closeConfirm(){document.getElementById('confirmOverlay').classList.remove('active');}

async function loadStats(){
  try{
    var[mr,sr,cr]=await Promise.all([fetch(API+'/movies'),fetch(API+'/series'),fetch(API+'/collections')]);
    var[m,s,c]=await Promise.all([mr.json(),sr.json(),cr.json()]);
    document.getElementById('statMovies').textContent=Array.isArray(m)?m.length:0;
    document.getElementById('statSeries').textContent=Array.isArray(s)?s.length:0;
    document.getElementById('statColls').textContent=Object.keys(c||{}).length;
  }catch(e){log('Stats failed: '+e.message,'error');}
}

async function downloadAll(){
  showToast('Downloading backups...','success');
  for(var type of['movies','series','collections']){
    try{
      var r=await fetch(API+'/'+type);var d=await r.json();
      var blob=new Blob([JSON.stringify(d,null,4)],{type:'application/json'});
      var a=document.createElement('a');a.href=URL.createObjectURL(blob);a.download=type+'_backup.json';a.click();
      await new Promise(function(res){setTimeout(res,400);});
    }catch(e){log('Backup '+type+' failed','error');}
  }
  log('All backups downloaded','success');showToast('Backups done!','success');
}

async function loadData(){
  try{
    var r=await fetch(API+'/'+view);var d=await r.json();
    if(Array.isArray(d)){originalData=d;}
    else{originalData=Object.entries(d).map(function(e){return Object.assign({},e[1],{id:e[0]});});}
    cachedData=originalData.slice();renderCards();
    log('Loaded '+cachedData.length+' '+view);
  }catch(e){log('Load failed: '+e.message,'error');}
}

function renderCards(){
  var grid=document.getElementById('cardsGrid'),html='';
  for(var item of cachedData){
    var meta='';
    if(view==='movies'){meta='<span>TMDB: '+(item.tmdb_id||'N/A')+'</span><span>'+(item.extras||'No extras')+'</span>';}
    else if(view==='series'){var eps=(item.seasons||[]).reduce(function(a,s){return a+(s.episodes||[]).length;},0);meta='<span>TMDB: '+(item.tmdb_id||'N/A')+'</span><span>'+((item.seasons||[]).length)+'S · '+eps+'ep</span>';}
    else{meta='<span>'+(item.name||'—')+'</span><span>'+((item.movies||[]).length)+' movies</span>';}
    var id=item.id.replace(/\\/g,'\\\\').replace(/'/g,"\\'");
    html+='<div class="item-card"><div class="card-header"><div class="card-title">'+item.id+'</div>'
      +'<div class="card-actions">'
      +'<button class="card-btn" onclick="editItem(\''+id+'\')" title="Edit"><svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><path d="M11 4H4a2 2 0 0 0-2 2v14a2 2 0 0 0 2 2h14a2 2 0 0 0 2-2v-7"/><path d="M18.5 2.5a2.121 2.121 0 0 1 3 3L12 15l-4 1 1-4 9.5-9.5z"/></svg></button>'
      +'<button class="card-btn delete" onclick="deleteItem(\''+id+'\')" title="Delete"><svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><polyline points="3 6 5 6 21 6"/><path d="M19 6v14a2 2 0 0 1-2 2H7a2 2 0 0 1-2-2V6m3 0V4a2 2 0 0 1 2-2h4a2 2 0 0 1 2 2v2"/></svg></button>'
      +'</div></div><div class="card-meta">'+meta+'</div></div>';
  }
  if(!html)html='<p style="color:var(--text-muted);grid-column:1/-1;text-align:center;padding:60px;background:var(--card);border-radius:12px;border:1px dashed var(--border);">No entries found</p>';
  grid.innerHTML=html;
}

function debounceSearch(){clearTimeout(searchTimer);searchTimer=setTimeout(function(){var q=document.getElementById('searchInput').value.toLowerCase().trim();cachedData=!q?originalData.slice():originalData.filter(function(i){return i.id.toLowerCase().includes(q)||(i.name||'').toLowerCase().includes(q);});renderCards();},180);}

function switchView(v){
  view=v;
  document.querySelectorAll('.nav-item').forEach(function(el){el.classList.remove('active');});
  document.getElementById('nav-'+v).classList.add('active');
  document.getElementById('pageTitle').textContent=v.charAt(0).toUpperCase()+v.slice(1);
  document.getElementById('moviesFields').style.display=v==='movies'?'block':'none';
  document.getElementById('seriesFields').style.display=v==='series'?'block':'none';
  document.getElementById('collectionsFields').style.display=v==='collections'?'block':'none';
  document.getElementById('searchInput').value='';
  if(window.innerWidth<=768)toggleSidebar();
  loadData();
}

function openModal(){
  isEdit=false;editingId=null;
  document.getElementById('modalTitle').textContent='New Entry';
  document.getElementById('deleteBtn').style.display='none';
  document.getElementById('entryForm').reset();
  ['subtitleRows','seasonsContainer','collMoviesContainer'].forEach(function(id){document.getElementById(id).innerHTML='';});
  resetPreview();
  document.getElementById('modalOverlay').classList.add('active');
}
function closeModal(){document.getElementById('modalOverlay').classList.remove('active');}
function resetPreview(){document.getElementById('previewBox').innerHTML='<svg width="48" height="48" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.5" style="color:var(--text-dim)"><rect x="3" y="3" width="18" height="18" rx="2"/><circle cx="8.5" cy="8.5" r="1.5"/><polyline points="21 15 16 10 5 21"/></svg>';document.getElementById('previewTitle').textContent='Image Preview';}

function editItem(id){
  isEdit=true;editingId=id;openModal();
  document.getElementById('modalTitle').textContent='Edit Entry';
  document.getElementById('deleteBtn').style.display='block';
  var item=originalData.find(function(x){return x.id===id;});
  if(!item)return;
  document.getElementById('mainId').value=item.id||'';
  document.getElementById('tmdbId').value=item.tmdb_id||'';
  if(view==='movies'){
    var dl=item.downloads||{};
    document.getElementById('m480').value=dl['480']||'';
    document.getElementById('m720').value=dl['720']||'';
    document.getElementById('m1080').value=dl['1080']||'';
    document.getElementById('mExtras').value=item.extras||'';
    document.getElementById('mPos').value=item.position||'top';
    if(item.subtitles)Object.entries(item.subtitles).forEach(function(e){addSubtitleRow({lang:e[0],url:e[1]});});
  }else if(view==='series'){
    (item.seasons||[]).forEach(function(s){addSeason(s);});
  }else{
    document.getElementById('cName').value=item.name||'';
    document.getElementById('cBanner').value=item.banner||'';
    document.getElementById('cBgm').value=item['bg-music']||'';
    (item.movies||[]).forEach(function(m){addCollMovieRow(m);});
  }
  if(item.tmdb_id)handleAutoFetch(item.tmdb_id);
}

function debounceAutoFetch(id){clearTimeout(fetchTimer);if(id&&id.length>=2)fetchTimer=setTimeout(function(){handleAutoFetch(id);},600);}
async function handleAutoFetch(id){
  if(!id||view==='collections')return;
  var type=view==='series'?'tv':'movie';
  try{
    var r=await fetch(TMDB_PROXY+'?path=/'+type+'/'+encodeURIComponent(id)+'%3F&language=en-US');
    var d=await r.json();
    if(d&&d.id){
      var title=d.title||d.name;
      if(d.poster_path){var imgUrl=d.poster_path.startsWith('http')?d.poster_path:'https://image.tmdb.org/t/p/w500'+d.poster_path;document.getElementById('previewBox').innerHTML='<img src="'+imgUrl+'">';}
      document.getElementById('previewTitle').textContent=title;
      if(!isEdit){var slug=title.toLowerCase().replace(/[^a-z0-9]+/g,'-').replace(/(^-|-$)/g,'');document.getElementById('mainId').value=slug;}
    }
  }catch(e){log('TMDB fetch failed','error');}
}

function addSubtitleRow(data){data=data||{};var div=document.createElement('div');div.className='sub-row';div.innerHTML='<input type="text" value="'+(data.lang||'')+'" placeholder="Language (e.g. en)"><input type="text" value="'+(data.url||'')+'" placeholder="Subtitle URL (.srt/.vtt)"><button type="button" class="del-btn" onclick="this.parentElement.remove()">✕</button>';document.getElementById('subtitleRows').appendChild(div);}
function addCollMovieRow(data){data=data||{};var div=document.createElement('div');div.className='coll-row';div.innerHTML='<input type="text" value="'+(data.id||'')+'" placeholder="Slug ID" class="r-id"><input type="number" value="'+(data.tmdb_id||'')+'" placeholder="TMDB" class="r-tmdb"><input type="text" value="'+(data.quality||'1080p')+'" placeholder="Quality" class="r-qual"><input type="text" value="'+(data.download||'')+'" placeholder="Download URL" class="r-url"><button type="button" class="del-btn" onclick="this.parentElement.remove()">✕</button>';document.getElementById('collMoviesContainer').appendChild(div);}

function addSeason(sData){
  var container=document.getElementById('seasonsContainer');var div=document.createElement('div');div.className='season-block';
  var sNum=sData&&sData.season_number?sData.season_number:(container.children.length+1);
  div.innerHTML='<div class="season-header"><input type="number" value="'+sNum+'" placeholder="Season #" class="s-num"><div style="display:flex;gap:10px;"><button type="button" class="btn btn-secondary" style="padding:8px 12px;font-size:.75rem;" onclick="quickAddEp(this,5)">+5 Eps</button><button type="button" class="btn btn-secondary" style="padding:8px 12px;font-size:.75rem;" onclick="addEpRow(this)">+1 Ep</button><button type="button" class="card-btn delete" style="width:38px;height:38px;" onclick="this.closest(\'.season-block\').remove()"><svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><polyline points="3 6 5 6 21 6"/><path d="M19 6v14a2 2 0 0 1-2 2H7a2 2 0 0 1-2-2V6"/></svg></button></div></div><div class="episodes"></div>';
  container.appendChild(div);
  if(sData&&sData.episodes&&sData.episodes.length)sData.episodes.forEach(function(ep){addEpRow(div,ep);});
  else addEpRow(div);
}
function quickAddEp(el,count){var block=el.closest('.season-block');var c=block.querySelector('.episodes');var start=c.children.length+1;for(var i=0;i<count;i++)addEpRow(block,{ep_number:start+i});}
function addEpRow(el,epData){
  var block=el.classList&&el.classList.contains('season-block')?el:el.closest('.season-block');
  var container=block.querySelector('.episodes');
  var epNum=epData&&epData.ep_number?epData.ep_number:(container.children.length+1);
  var links=(epData&&epData.links)||{};
  // subtitles stored as {lang:url} dict — serialise back to lang|url lines for editing
  var subsObj=epData&&epData.subtitles||{};
  var subsText=Object.entries(subsObj).map(function(e){return e[0]+'|'+e[1];}).join('\n');
  var div=document.createElement('div');div.className='ep-row';
  div.innerHTML='<input type="number" value="'+epNum+'" placeholder="Ep#" class="ep-num">'
    +'<input type="text" value="'+(links['360p']||'')+'" placeholder="360p URL" class="ep-360">'
    +'<input type="text" value="'+(links['720p']||'')+'" placeholder="720p URL" class="ep-720">'
    +'<input type="text" value="'+(links['1080p']||'')+'" placeholder="1080p URL" class="ep-1080">'
    +'<input type="text" value="'+subsText.replace(/"/g,'&quot;')+'" placeholder="Subtitles: lang|url (one per line)" class="ep-sub">'
    +'<button type="button" class="del-btn" onclick="this.parentElement.remove()">✕</button>';
  container.appendChild(div);
}

async function saveEntry(){
  var saveBtn=document.getElementById('saveBtn');saveBtn.disabled=true;
  var mainId=document.getElementById('mainId').value.trim();
  if(!mainId){showToast('Please enter a Slug ID','error');saveBtn.disabled=false;return;}
  var payload;
  if(view==='movies'){
    var subs={};
    document.querySelectorAll('.sub-row').forEach(function(r){var l=r.children[0].value.trim(),u=r.children[1].value.trim();if(l&&u)subs[l]=u;});
    var dl={};
    var v480=document.getElementById('m480').value.trim();
    var v720=document.getElementById('m720').value.trim();
    var v1080=document.getElementById('m1080').value.trim();
    if(v480)dl['480']=v480;if(v720)dl['720']=v720;if(v1080)dl['1080']=v1080;
    payload={id:mainId,tmdb_id:parseInt(document.getElementById('tmdbId').value)||null,extras:document.getElementById('mExtras').value,position:document.getElementById('mPos').value,downloads:dl,subtitles:subs};
  }else if(view==='series'){
    var seasons=[];
    document.querySelectorAll('.season-block').forEach(function(block){
      var sNum=parseInt(block.querySelector('.s-num').value)||0;
      var episodes=[];
      block.querySelectorAll('.ep-row').forEach(function(ep){
        var epNum=parseInt(ep.querySelector('.ep-num').value)||0;
        if(epNum){
          var subsRaw=ep.querySelector('.ep-sub').value||'';
          var subsDict={};
          subsRaw.split('\n').forEach(function(line){
            line=line.trim();if(!line)return;
            var idx=line.indexOf('|');
            if(idx>0){subsDict[line.slice(0,idx).trim().toLowerCase()]=line.slice(idx+1).trim();}
            else if(line){subsDict['en']=line;}
          });
          episodes.push({ep_number:epNum,links:{'360p':ep.querySelector('.ep-360').value||'','720p':ep.querySelector('.ep-720').value||'','1080p':ep.querySelector('.ep-1080').value||''},subtitles:subsDict});
        }
      });
      seasons.push({season_number:sNum,episodes:episodes});
    });
    payload={id:mainId,tmdb_id:document.getElementById('tmdbId').value,seasons:seasons};
  }else{
    var movies=[];
    document.querySelectorAll('.coll-row').forEach(function(r){
      var mid=r.querySelector('.r-id').value.trim(),murl=r.querySelector('.r-url').value.trim();
      if(mid&&murl)movies.push({id:mid,tmdb_id:parseInt(r.querySelector('.r-tmdb').value)||0,quality:r.querySelector('.r-qual').value,download:murl});
    });
    payload={id:mainId,name:document.getElementById('cName').value,banner:document.getElementById('cBanner').value,'bg-music':document.getElementById('cBgm').value,movies:movies};
  }
  try{
    var resp;
    // Backend uses POST as upsert for both create and update — no PUT support
    resp=await fetch(API+'/'+view,{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify(payload)});
    if(resp.ok){closeModal();loadData();loadStats();showToast('Saved "'+mainId+'"','success');log('Saved: '+mainId,'success');}
    else{var err=await resp.json().catch(function(){return{};});showToast('Save failed: '+(err.error||resp.status),'error');log('Save failed','error');}
  }catch(e){showToast('Error: '+e.message,'error');log('Error: '+e.message,'error');}
  saveBtn.disabled=false;
}

function askDelete(){showConfirm('Delete Entry','Delete "'+editingId+'"? This cannot be undone.',function(){deleteItem(editingId);});}
async function deleteItem(id){
  try{
    var r=await fetch(API+'/'+view+'/'+id,{method:'DELETE'});
    if(r.ok){loadData();loadStats();closeModal();showToast('Deleted "'+id+'"','success');log('Deleted: '+id,'success');}
    else{showToast('Delete failed','error');log('Delete failed','error');}
  }catch(e){showToast('Delete failed','error');}
}

function init(){
  fetch(window.location.origin+'/health').then(function(){
    document.getElementById('statusDot').classList.add('online');
    document.getElementById('statusText').textContent='Online';
    loadData();loadStats();log('Admin panel ready','success');
  }).catch(function(){
    document.getElementById('statusText').textContent='Offline';
    log('Server offline','error');
  });
}

document.getElementById('modalOverlay').addEventListener('click',function(e){if(e.target===this)closeModal();});
document.addEventListener('keydown',function(e){if(e.key==='Escape'){closeModal();closeConfirm();}});
init();
</script>
</body>
</html>
"""

async def web_admin(req: web.Request) -> web.Response:
    if (err := _token_required(req)):
        return err
    html = _ADMIN_HTML.replace("__BACKEND_URL__", BACKEND_URL)
    return web.Response(text=html, content_type="text/html")
