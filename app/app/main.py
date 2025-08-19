import os
import time
import random
from typing import List, Tuple, Optional, Dict, Any

import httpx
from fastapi import FastAPI, Request, HTTPException
from fastapi.responses import HTMLResponse, RedirectResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates

# =========================
#   Configuración nodos LL
# =========================

RAW_NODES = os.getenv("LAVA_NODES", "").split(",")
LAVA_TIMEOUT = int(os.getenv("LAVA_TIMEOUT_MS", "8000"))  # ms

def extract_tracks(obj: Dict[str, Any]) -> List[Dict[str, Any]]:
    # v3: "tracks", v4: "data"
    return obj.get("tracks") or obj.get("data") or []

def parse_node(s: str) -> Optional[Tuple[str, str]]:
    """
    Acepta:
      - 'https://host:443|password'
      - 'https://host:443:password' (fallback)
    """
    s = s.strip()
    if not s:
        return None
    if "|" in s:
        url, password = s.split("|", 1)
        return url.strip(), password.strip()
    parts = s.split(":")
    if len(parts) >= 3:
        url = ":".join(parts[:-1])
        password = parts[-1]
        return url.strip(), password.strip()
    return None

NODES: List[Tuple[str, str]] = [n for n in (parse_node(x) for x in RAW_NODES) if n]

# Cache salud nodos para no golpear /v4/info todo el rato
_health_cache: Dict[str, Tuple[bool, float]] = {}  # base -> (ok, ts)

async def check_node(client: httpx.AsyncClient, base: str, password: str) -> bool:
    try:
        r = await client.get(f"{base}/v4/info",
                             headers={"Authorization": password},
                             timeout=LAVA_TIMEOUT / 1000)
        return r.status_code == 200
    except Exception:
        return False

async def pick_node() -> Tuple[str, str]:
    if not NODES:
        raise HTTPException(500, "No hay nodos en LAVA_NODES")
    candidates = NODES[:]
    random.shuffle(candidates)
    async with httpx.AsyncClient() as client:
        for base, password in candidates:
            ok, ts = _health_cache.get(base, (False, 0))
            if ok and (time.time() - ts) < 20:
                return base, password
            if await check_node(client, base, password):
                _health_cache[base] = (True, time.time())
                return base, password
            _health_cache[base] = (False, time.time())
    raise HTTPException(502, "Todos los nodos Lavalink fallan en /v4/info")

# =========================
#   Resolución audio (Piped + Invidious)
# =========================

PIPED_INSTANCES = [
    s.strip() for s in os.getenv(
        "PIPED_INSTANCES",
        "https://piped.video,https://piped.mha.fi,https://piped.garudalinux.org,https://piped.projectsegfau.lt,https://piped.lunar.icu"
    ).split(",") if s.strip()
]

INVIDIOUS_INSTANCES = [
    s.strip() for s in os.getenv(
        "INVIDIOUS_INSTANCES",
        "https://yewtu.be,https://invidious.lunar.icu,https://inv.nadeko.net,https://invidious.protokolla.fi"
    ).split(",") if s.strip()
]

COMMON_HEADERS = {
    "User-Agent": ("Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
                   "(KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36"),
    "Accept": "application/json,text/plain,*/*",
}

async def resolve_piped_audio(vid: str) -> Optional[str]:
    """
    Devuelve una URL directa (m4a/webm) para un video de YouTube usando Piped.
    """
    async with httpx.AsyncClient(follow_redirects=True, headers=COMMON_HEADERS) as client:
        for base in PIPED_INSTANCES:
            try:
                r = await client.get(f"{base}/streams/{vid}", timeout=8)
                if r.status_code != 200:
                    continue
                js = r.json()
                streams = js.get("audioStreams") or []
                if not streams:
                    continue
                # Preferimos audio/mp4 (m4a). Si no, el mayor bitrate.
                def key(s): return (("audio/mp4" in (s.get("mimeType") or "")), s.get("bitrate") or 0)
                best = sorted(streams, key=key, reverse=True)[0]
                url = best.get("url")
                if url:
                    return url
            except Exception:
                continue
    return None

async def resolve_invidious_audio(vid: str) -> Optional[str]:
    """
    Fallback usando Invidious: /api/v1/videos/{id}. Escogemos el mejor solo‑audio.
    """
    async with httpx.AsyncClient(follow_redirects=True, headers=COMMON_HEADERS) as client:
        for base in INVIDIOUS_INSTANCES:
            try:
                r = await client.get(f"{base}/api/v1/videos/{vid}", timeout=8)
                if r.status_code != 200:
                    continue
                js = r.json()
                cands = (js.get("adaptiveFormats") or []) + (js.get("formatStreams") or [])
                audio = [c for c in cands if "audio" in (c.get("type") or c.get("mimeType") or "").lower()]
                if not audio:
                    audio = [c for c in cands if (c.get("bitrate") and not c.get("qualityLabel"))]
                if not audio:
                    continue
                def key(s): return (("audio/mp4" in (s.get("type") or s.get("mimeType") or "")), s.get("bitrate") or 0)
                best = sorted(audio, key=key, reverse=True)[0]
                u = best.get("url")
                if u:
                    return u
            except Exception:
                continue
    return None

async def resolve_audio_url(vid: str) -> Optional[str]:
    # 1) Piped
    url = await resolve_piped_audio(vid)
    if url:
        return url
    # 2) Invidious
    url = await resolve_invidious_audio(vid)
    if url:
        return url
    return None

# =========================
#   Búsquedas (Lavalink)
# =========================

async def to_lavalink_tracks(identifier: str) -> Dict[str, Any]:
    """
    Hace GET /v4/loadtracks en el nodo elegido. Si no hay resultados con ytsearch,
    prueba automáticamente ytmsearch (y viceversa).
    """
    async def _load(idf: str) -> Dict[str, Any]:
        base, password = await pick_node()
        async with httpx.AsyncClient() as client:
            r = await client.get(
                f"{base}/v4/loadtracks",
                params={"identifier": idf},
                headers={"Authorization": password},
                timeout=LAVA_TIMEOUT / 1000,
            )
        if r.status_code != 200:
            raise HTTPException(r.status_code, f"loadtracks falló: {r.text}")
        return r.json()

    data = await _load(identifier)
    if extract_tracks(data):
        return data

    # fallback yt <-> ytm
    if identifier.startswith("ytsearch:"):
        return await _load("ytmsearch:" + identifier[len("ytsearch:"):])
    if identifier.startswith("ytmsearch:"):
        return await _load("ytsearch:" + identifier[len("ytmsearch:"):])

    return data

async def search_source_list(source: str, query: str) -> List[Dict[str, Any]]:
    """
    Devuelve lista de candidatos con 'identifier' que luego convertimos con /v4/loadtracks.
    """
    source = source.lower()
    if source == "yt":
        return [{"display": f"YouTube: {query}", "identifier": f"ytsearch:{query}"}]
    elif source == "sp":
        return [{"display": f"Spotify→YT: {query}", "identifier": f"ytsearch:{query}"}]
    elif source == "dz":
        out: List[Dict[str, Any]] = []
        try:
            async with httpx.AsyncClient() as client:
                r = await client.get("https://api.deezer.com/search",
                                     params={"q": query}, timeout=6)
                data = r.json()
                for d in (data.get("data") or [])[:10]:
                    title = d.get("title") or ""
                    artist = (d.get("artist") or {}).get("name") or ""
                    cover = (d.get("album") or {}).get("cover_medium")
                    if title and artist:
                        out.append({
                            "display": f"{artist} — {title}",
                            "identifier": f"ytsearch:{artist} - {title}",
                            "cover": cover
                        })
        except Exception:
            pass
        if not out:
            out.append({"display": f"Deezer→YT: {query}", "identifier": f"ytsearch:{query}"})
        return out
    else:
        raise HTTPException(400, "source debe ser yt|sp|dz")

# =========================
#   FastAPI + UI
# =========================

app = FastAPI()
app.mount("/static", StaticFiles(directory="app/static"), name="static")
templates = Jinja2Templates(directory="app/templates")

@app.get("/", response_class=HTMLResponse)
async def index(request: Request):
    return templates.TemplateResponse("index.html", {"request": request})

@app.get("/api/search")
async def api_search(source: str, q: str):
    """
    1) Construye candidatos (yt/sp/dz) → 'identifier'
    2) Pide a Lavalink /v4/loadtracks
    3) Devuelve lista de pistas (encoded+info); si sourceName=youtube, añade 'vid'
    """
    candidates = await search_source_list(source, q)
    final: List[Dict[str, Any]] = []

    for cand in candidates:
        try:
            data = await to_lavalink_tracks(cand["identifier"])
            tracks = extract_tracks(data)
            if not tracks:
                continue
            t0 = tracks[0]
            info = t0.get("info", {})
            vid = info.get("identifier") if info.get("sourceName") == "youtube" else None
            final.append({
                "display": cand.get("display"),
                "encoded": t0.get("encoded"),
                "info": info,
                "cover": cand.get("cover"),
                "vid": vid,
            })
        except Exception:
            continue

    return {"results": final}

@app.get("/api/resolve")
async def api_resolve(vid: str):
    """
    Devuelve URL directa de audio para un videoId de YouTube (Piped → Invidious).
    """
    url = await resolve_audio_url(vid)
    if not url:
        raise HTTPException(404, f"No se pudo resolver audio para {vid} (Piped/Invidious)")
    return {"url": url}

@app.get("/audio/{vid}")
async def audio_redirect(vid: str):
    """
    Endpoint estable para el <audio>: redirige con 307 a la URL directa.
    """
    url = await resolve_audio_url(vid)
    if not url:
        raise HTTPException(404, f"No se pudo resolver audio para {vid} (Piped/Invidious)")
    return RedirectResponse(url, status_code=307)

# =========================
#   Debug
# =========================

from datetime import datetime

@app.get("/api/debug/nodes")
async def debug_nodes():
    results = []
    async with httpx.AsyncClient() as client:
        for base, password in NODES:
            started = time.perf_counter()
            item = {"base": base, "ok": False, "error": None, "latency_ms": None, "version": None}
            try:
                r = await client.get(f"{base}/v4/info",
                                     headers={"Authorization": password},
                                     timeout=LAVA_TIMEOUT/1000)
                item["latency_ms"] = round((time.perf_counter() - started) * 1000, 1)
                item["ok"] = (r.status_code == 200)
                if item["ok"]:
                    js = r.json()
                    item["version"] = js.get("version") or js.get("build")
                else:
                    item["error"] = f"HTTP {r.status_code}: {r.text[:200]}"
            except Exception as e:
                item["error"] = repr(e)
            results.append(item)
    return {"when": datetime.utcnow().isoformat() + "Z",
            "nodes": results,
            "env_nodes": RAW_NODES}

@app.get("/api/debug/load")
async def api_debug_load(identifier: str):
    base, password = await pick_node()
    async with httpx.AsyncClient() as client:
        r = await client.get(f"{base}/v4/loadtracks",
                             params={"identifier": identifier},
                             headers={"Authorization": password},
                             timeout=LAVA_TIMEOUT/1000)
    return {
        "node": base,
        "status": r.status_code,
        "text": r.text[:2000]
    }

@app.get("/api/debug/pick")
async def api_debug_pick():
    base, _ = await pick_node()
    return {"node": base}
