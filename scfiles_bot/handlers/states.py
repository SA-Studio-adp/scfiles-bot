"""handlers/states.py — every ConversationHandler state constant, in one
place so handler modules can import just the ones they need without
risking two modules picking the same integer."""

# ── conversation states ───────────────────────────────────────────────────────
(
    AM_TMDB, AM_EXTRA, AM_DL480, AM_DL720, AM_DL1080, AM_POS, AM_CONFIRM,
    AS_TMDB, AS_SN, AS_EP, AS_EP360, AS_EP720, AS_EP_MORE,
    AC_ID, AC_NAME, AC_BANNER, AC_BGMUSIC,
    AC_MOV_TMDB, AC_MOV_QUAL, AC_MOV_DL, AC_MOV_MORE,
    DM_ID, DS_ID, DC_ID,
    EM_ID, EM_VALUE,
    ESS_ID, ESS_ACTION, ESS_SN, ESS_EP, ESS_EP360, ESS_EP720, ESS_EP_MORE, ESS_DEL_EP,
    TQ_TYPE, TQ_QUERY,
    # v3 additions ↓
    AM_SUB,                                              # addmovie: subtitles step
    AS_EP1080, AS_EP_SUB,                                # addseries: 1080p + subtitle step
    ESS_SN_PICK, ESS_EP1080, ESS_EDIT_PICK, ESS_EP_SUB, # editseries: season buttons + edit + 1080p + subtitle
    EC_ID, EC_ACTION, EC_FIELD_VALUE,                    # editcollection
    EC_MOV_TMDB, EC_MOV_QUAL, EC_MOV_DL, EC_DEL_MOV_PICK,
) = range(50)
