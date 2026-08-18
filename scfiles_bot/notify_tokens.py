"""
notify_tokens.py - short-lived, in-memory tokens for the notify-ask flow.

Each upload's "Send a notification?" prompt gets its own token, mapping to
that specific item's data (kind/item/poster_url/chat_id). This lets
multiple prompts be pending at once (e.g. you upload three things in a
row before resolving any of their notify prompts) without one overwriting
another via a shared ctx.user_data slot - and lets the Schedule webapp
look up exactly what it's scheduling via a token in the URL, entirely
independent of Telegram's bot-conversation state machine.

Single-process, in-memory by design - tokens don't need to survive a
restart; anything abandoned just expires and is swept up periodically
(see sweep_expired, called from scheduler.job_send_scheduled_notifications).
"""
import secrets
import time

_TOKENS = {}
_TOKEN_TTL_SECONDS = 24 * 3600  # a token (and its unresolved prompt) is good for 24h


def create(data: dict) -> str:
    token = secrets.token_urlsafe(16)
    _TOKENS[token] = dict(data, _created_at=time.time())
    return token

def get(token: str):
    entry = _TOKENS.get(token)
    if entry is None:
        return None
    if time.time() - entry["_created_at"] > _TOKEN_TTL_SECONDS:
        _TOKENS.pop(token, None)
        return None
    return entry

def update(token: str, **fields) -> bool:
    """Merges extra fields into an existing token's data (e.g. recording
    which message the prompt was sent as, once it's actually been sent —
    the token has to exist before the message can reference it in its
    button URL, so this fills in the message_id afterward). Returns False
    if the token doesn't exist / already expired."""
    entry = get(token)
    if entry is None:
        return False
    entry.update(fields)
    return True

def pop(token: str):
    entry = get(token)
    _TOKENS.pop(token, None)
    return entry

def sweep_expired() -> int:
    now = time.time()
    expired = [t for t, e in _TOKENS.items() if now - e["_created_at"] > _TOKEN_TTL_SECONDS]
    for t in expired:
        _TOKENS.pop(t, None)
    return len(expired)
