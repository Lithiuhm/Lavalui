import os
import time
import random
import re
from typing import List, Tuple, Optional, Dict, Any
import asyncio
from datetime import datetime
import ipaddress

import httpx
from fastapi import FastAPI, Request, HTTPException, Header
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import (
    HTMLResponse, RedirectResponse, StreamingResponse, Response, FileResponse
)
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates

# =========================
#   Configuración general
# =========================

RAW_NODES = os.getenv("LAVA_NODES", "").split(",")
LAVA_TIMEOUT = int(os.getenv("LAVA_TIMEOUT_MS", "8000"))  # ms

PIPED_INSTANCES = [s.strip() for s in os.getenv(
    "PIPED_INSTANCES",
    "https://piped.mha.fi,https://piped.garudalinux.org,https://piped.lunar.icu,https://piped.video"
).split(",") if s.strip()]

INVIDIOUS_INSTANCES = [s.strip() for s in os.getenv(
    "INVIDIOUS_INSTANCES",
    "https://yewtu.be,https://iv.ggtyler.dev,https://invidious.slipfox.xyz,https://invidious.fdn.fr"
).split(",") if s.strip()]

RESOLVE_TIMEOUT = float(os.getenv("RESOLVE_TIMEOUT", "8"))  # s
CACHE_TTL = int(os.getenv("RESOLVE_CACHE_TTL", "600"))      # s
_UA = {"User-Agent": "Mozilla/5.0 (compatible; lavalui/1.0)"}
_ITAGS = [140, 251, 171, 250, 249]  # 140=m4a/aac, 251=webm/opus...

# =========================
#   Utilidades Lavalink
# =========================

def extract_tracks(obj: Dict[str, Any]) -> List[Dict[str, Any]]:
    # v3: "tracks", v4: "data"
    return obj.get("tracks") or obj.get("data") or []

def parse_node(s: str) -> Optional[Tuple[str, str]]:
    """
    Formatos:
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
#   Resolución audio público (NO youtube.com)
# =========================

_resolve_cache: Dict[str, Tuple[float, str]] = {}  # vid -> (exp_ts, url)

async def _try_piped(client: httpx.AsyncClient, base: str, vid: str) -> Optional[str]:
    try:
        r = await client.get(f"{base}/streams/{vid}", headers=_UA, timeout=RESOLVE_TIMEOUT)
        if r.status_code != 200:
            return None
        js = r.json()
        streams = js.get("audioStreams") or []
        if not streams:
            return None
        # prioriza m4a/aac si hay; si no, el mayor bitrate
        m4a = [s for s in streams if ('m4a' in (s.get('mimeType') or '').lower()) or ('audio/mp4' in (s.get('mimeType') or '').lower())]
        if m4a:
            m4a.sort(key=lambda s: s.get('bitrate', 0) or 0, reverse=True)
            return m4a[0].get("url")
        best = max(streams, key=lambda s: s.get("bitrate", 0) or 0)
        return best.get("url")
    except Exception:
        return None

async def _try_invidious_latest(client: httpx.AsyncClient, base: str, vid: str) -> Optional[str]:
    for itag in _ITAGS:
        url = f"{base}/latest_version?id={vid}&itag={itag}&local=true"
        try:
            r = await client.get(url, headers=_UA, timeout=RESOLVE_TIMEOUT, follow_redirects=True)
            if r.status_code < 400:
                return url
        except Exception:
            continue
    return None

async def resolve_public_audio_url(vid: str) -> Optional[str]:
    """
    Devuelve una URL reproducible de Piped o Invidious.
    Nunca devuelve un host de youtube.com.
    """
    now = time.time()
    cached = _resolve_cache.get(vid)
    if cached and cached[0] > now:
        return cached[1]

    async with httpx.AsyncClient(follow_redirects=True) as client:
        # 1) Piped
        tasks = [_try_piped(client, b, vid) for b in PIPED_INSTANCES]
        for coro in asyncio.as_completed(tasks, timeout=RESOLVE_TIMEOUT + 2):
            try:
                url = await coro
                if url:
                    _resolve_cache[vid] = (now + CACHE_TTL, url)
                    return url
            except Exception:
                pass

        # 2) Invidious
        tasks = [_try_invidious_latest(client, b, vid) for b in INVIDIOUS_INSTANCES]
        for coro in asyncio.as_completed(tasks, timeout=RESOLVE_TIMEOUT + 2):
            try:
                url = await coro
                if url:
                    _resolve_cache[vid] = (now + CACHE_TTL, url)
                    return url
            except Exception:
                pass

    return None

# =========================
#   Búsquedas (Lavalink)
# =========================

async def to_lavalink_tracks(identifier: str) -> Dict[str, Any]:
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

    if identifier.startswith("ytsearch:"):
        return await _load("ytmsearch:" + identifier[len("ytsearch:"):])
    if identifier.startswith("ytmsearch:"):
        return await _load("ytsearch:" + identifier[len("ytmsearch:"):])

    return data

async def search_source_list(source: str, query: str) -> List[Dict[str, Any]]:
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
    elif source == "sc":
        return [{"display": f"SoundCloud: {query}", "identifier": f"scsearch:{query}"}]
    elif source == "bc":
        return [{"display": f"Bandcamp→YT: {query}", "identifier": f"ytsearch:{query}"}]
    else:
        raise HTTPException(400, "source debe ser yt|sp|dz|sc|bc")

# =========================
#   FastAPI + UI
# =========================

app = FastAPI()
app.add_middleware(
    CORSMiddleware,
    allow_origins=["https://music.santiagoac.com","http://localhost:8000","*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)
app.mount("/static", StaticFiles(directory="app/static"), name="static")
templates = Jinja2Templates(directory="app/templates")

@app.get("/", response_class=HTMLResponse)
async def index(request: Request):
    return templates.TemplateResponse("index.html", {"request": request})

@app.get("/healthz")
async def healthz():
    return {"ok": True, "time": datetime.utcnow().isoformat()+"Z"}

@app.get("/api/search")
async def api_search(source: str, q: str):
    candidates = await search_source_list(source, q)
    final: List[Dict[str, Any]] = []

    for cand in candidates:
        try:
            data = await to_lavalink_tracks(cand["identifier"])
            tracks = extract_tracks(data)
            if not tracks:
                continue

            for t in tracks[:10]:
                info = t.get("info", {}) or {}
                vid = None
                if info.get("sourceName") == "youtube":
                    ident = info.get("identifier") or ""
                    if len(ident) == 11:
                        vid = ident

                final.append({
                    "display": cand.get("display"),
                    "encoded": t.get("encoded"),
                    "info": {
                        "title": info.get("title"),
                        "author": info.get("author"),
                        "length": info.get("length"),
                        "uri": info.get("uri"),
                        "sourceName": info.get("sourceName"),
                        "artworkUrl": info.get("artworkUrl"),
                    },
                    "cover": cand.get("cover"),
                    "vid": vid,
                })
        except Exception:
            continue

    return {"results": final}

# =========================
#   Streaming proxy directo (sigue disponible)
# =========================

async def _list_candidates(vid: str) -> List[str]:
    urls: List[str] = []
    # Piped
    async with httpx.AsyncClient(follow_redirects=True, headers=_UA, timeout=RESOLVE_TIMEOUT) as c:
        for base in PIPED_INSTANCES:
            try:
                r = await c.get(f"{base}/streams/{vid}")
                if r.status_code != 200:
                    continue
                js = r.json()
                streams = js.get("audioStreams") or []
                if not streams:
                    continue
                m4a = [s for s in streams if ('m4a' in (s.get('mimeType') or '').lower()) or ('audio/mp4' in (s.get('mimeType') or '').lower())]
                if m4a:
                    m4a.sort(key=lambda s: s.get('bitrate', 0) or 0, reverse=True)
                    if m4a[0].get('url'):
                        urls.append(m4a[0]['url'])
                else:
                    best = max(streams, key=lambda s: s.get("bitrate", 0) or 0)
                    if best.get('url'):
                        urls.append(best['url'])
            except Exception:
                continue
    # Invidious (últimos)
    for base in INVIDIOUS_INSTANCES:
        for it in _ITAGS:
            urls.append(f"{base}/latest_version?id={vid}&itag={it}&local=true")

    # únicos
    seen, uniq = set(), []
    for u in urls:
        if u in seen: continue
        seen.add(u); uniq.append(u)
    return uniq

async def _open_stream_validated(url: str, range_header: Optional[str]) -> Tuple[httpx.Response, httpx.AsyncClient]:
    client = httpx.AsyncClient(follow_redirects=True, headers=_UA, timeout=None, http2=False)
    headers = {}
    if range_header:
        headers["Range"] = range_header
    try:
        req = client.build_request("GET", url, headers=headers)
        resp = await client.send(req, stream=True)
        if resp.status_code >= 400:
            await resp.aclose(); await client.aclose()
            raise HTTPException(502, f"upstream {resp.status_code}")
        return resp, client
    except Exception:
        try: await client.aclose()
        except Exception: pass
        raise

@app.get("/audio-proxy/{vid}")
async def audio_proxy(vid: str, request: Request):
    range_header = request.headers.get("range")
    candidates = await _list_candidates(vid)
    if not candidates:
        raise HTTPException(404, f"No hay fuentes públicas para {vid}")

    last_err = None
    for url in candidates[:10]:
        try:
            resp, client = await _open_stream_validated(url, range_header)
        except HTTPException as e:
            last_err = e; continue
        except Exception as e:
            last_err = HTTPException(502, repr(e)); continue

        if range_header and not resp.headers.get("content-range"):
            try:
                await resp.aclose(); await client.aclose()
            except Exception: pass
            try:
                resp, client = await _open_stream_validated(url, None)
            except Exception as e:
                last_err = HTTPException(502, f"no soporta Range y fallo sin Range: {e!r}")
                continue

        upstream_ct = (resp.headers.get("content-type") or "").lower()
        ct = upstream_ct if (upstream_ct and not upstream_ct.startswith("text/")) else "audio/webm"
        low_url = url.lower()
        if "itag=140" in low_url or "audio/mp4" in upstream_ct or low_url.endswith(".m4a"):
            ct = "audio/mp4"

        status = 206 if resp.headers.get("content-range") else 200
        out_headers = {
            "accept-ranges": "bytes",
            "cache-control": "no-store",
            "content-type": ct,
            "x-accel-buffering": "no",
            "content-disposition": "inline",
        }
        if resp.headers.get("content-range"):
            out_headers["content-range"] = resp.headers["content-range"]

        async def body_iter():
            try:
                async for chunk in resp.aiter_bytes(64 * 1024):
                    if await request.is_disconnected(): break
                    yield chunk
            finally:
                try:
                    await resp.aclose()
                finally:
                    await client.aclose()

        return StreamingResponse(body_iter(), status_code=status, headers=out_headers, media_type=ct)

    if last_err: raise last_err
    raise HTTPException(502, "No se pudo abrir ninguna fuente de audio")

# Redirect simple (por si quieres abrir en pestaña)
@app.get("/audio/{vid}")
async def audio_redirect(vid: str):
    origin = await resolve_public_audio_url(vid)
    if not origin:
        raise HTTPException(404, f"No se pudo resolver audio para {vid}")
    return RedirectResponse(url=origin, status_code=302)

# =========================
#   HLS Fallback (opcional)
# =========================

HLS_ROOT = "/tmp/hls_lavalui"
os.makedirs(HLS_ROOT, exist_ok=True)
_hls_index: Dict[str, Dict[str, Any]] = {}

async def _ensure_hls_build(vid: str) -> Tuple[str, str]:
    for token, meta in list(_hls_index.items()):
        if meta.get("vid") == vid and meta["expires"] > time.time():
            return token, f"/hls/{token}/index.m3u8"

    origin = await resolve_public_audio_url(vid)
    if not origin:
        raise HTTPException(404, f"No se pudo resolver audio para {vid}")

    token = f"{vid}-{int(time.time())}-{random.randint(1000,9999)}"
    out_dir = os.path.join(HLS_ROOT, token)
    os.makedirs(out_dir, exist_ok=True)

    cmd = [
        "ffmpeg", "-y", "-v", "quiet",
        "-i", origin,
        "-vn",
        "-c:a", "aac", "-b:a", "128k",
        "-movflags", "+faststart",
        "-f", "hls",
        "-hls_time", "4",
        "-hls_playlist_type", "event",
        "-hls_segment_filename", os.path.join(out_dir, "seg%03d.ts"),
        os.path.join(out_dir, "index.m3u8"),
    ]
    await asyncio.create_subprocess_exec(*cmd)
    _hls_index[token] = {"dir": out_dir, "expires": time.time() + 3600, "vid": vid}
    return token, f"/hls/{token}/index.m3u8"

@app.get("/audio-hls/{vid}")
async def audio_hls_entry(vid: str):
    token, rel = await _ensure_hls_build(vid)
    return RedirectResponse(url=rel, status_code=302)

@app.get("/hls/{token}/{fname}")
async def serve_hls(token: str, fname: str):
    meta = _hls_index.get(token)
    if not meta or meta["expires"] < time.time():
        raise HTTPException(404, "HLS expirado")
    if not re.match(r"^(index\.m3u8|seg\d{3}\.ts)$", fname):
        raise HTTPException(400, "Nombre de fichero inválido")

    path = os.path.join(meta["dir"], fname)
    if not os.path.isfile(path):
        raise HTTPException(404, "HLS no listo")

    if fname.endswith(".m3u8"):
        return FileResponse(path, media_type="application/vnd.apple.mpegurl")
    return FileResponse(path, media_type="video/MP2T")

# =========================
#   Preparación local (M4A) con límites
# =========================

PREP_ROOT = os.getenv("PREP_ROOT", "/tmp/prep_audio")
os.makedirs(PREP_ROOT, exist_ok=True)

MAX_SECONDS = int(os.getenv("PREP_MAX_SECONDS", "7200"))      # 2h
CLEANUP_TTL = int(os.getenv("PREP_CLEANUP_TTL", "7200"))      # 2h
MAX_CONCURRENT = int(os.getenv("PREP_MAX_CONCURRENT", "2"))   # prepara 2 a la vez

# rate limit muy simple: ip -> [t1,t2,...] (timestamps de peticiones POST /prepare)
RL_WINDOW = int(os.getenv("PREP_RL_WINDOW", "60"))  # seg
RL_MAX = int(os.getenv("PREP_RL_MAX", "6"))         # máx inicios por ventana

_prep_jobs: Dict[str, Dict[str, Any]] = {}
_prep_sem = asyncio.Semaphore(MAX_CONCURRENT)
_rl_hits: Dict[str, List[float]] = {}

def _client_ip(req: Request) -> str:
    # intenta X-Forwarded-For por Traefik/CF; cae a client.host
    xf = req.headers.get("x-forwarded-for", "")
    if xf:
        ip = xf.split(",")[0].strip()
    else:
        ip = req.client.host if req.client else "0.0.0.0"
    # sanity
    try:
        ipaddress.ip_address(ip)
    except Exception:
        ip = "0.0.0.0"
    return ip

def _rate_limit_check(ip: str) -> bool:
    now = time.time()
    arr = _rl_hits.get(ip, [])
    arr = [t for t in arr if now - t < RL_WINDOW]
    if len(arr) >= RL_MAX:
        _rl_hits[ip] = arr
        return False
    arr.append(now)
    _rl_hits[ip] = arr
    return True

def _token_for(vid: str) -> str:
    return f"{vid}-{int(time.time())}-{random.randint(1000,9999)}"

async def _ffmpeg_prepare(origin_url: str, out_path: str) -> Tuple[bool, str]:
    cmd = [
        "ffmpeg", "-y", "-v", "error",
        "-i", origin_url,
        "-vn",
        "-c:a", "aac", "-b:a", "160k",
        "-movflags", "+faststart",
        "-t", str(MAX_SECONDS),
        out_path,
    ]
    proc = await asyncio.create_subprocess_exec(
        *cmd,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
    )
    _, err = await proc.communicate()
    ok = (proc.returncode == 0 and os.path.exists(out_path) and os.path.getsize(out_path) > 0)
    if not ok:
        try: os.remove(out_path)
        except Exception: pass
    return ok, (err.decode("utf-8", "ignore") if err else "")

async def _run_prep_job(token: str, vid: str):
    job = _prep_jobs[token]
    job["status"] = "resolving"
    try:
        origin = await resolve_public_audio_url(vid)
        if not origin:
            job["status"] = "error"; job["msg"] = "No se pudo resolver una URL pública"; return

        job["status"] = "queued"
        async with _prep_sem:
            job["status"] = "downloading"
            out_path = os.path.join(PREP_ROOT, f"{token}.m4a")
            ok, fferr = await _ffmpeg_prepare(origin, out_path)
            if not ok:
                job["status"] = "error"; job["msg"] = f"ffmpeg falló: {fferr[:200]}"; return
            job["status"] = "ready"; job["path"] = out_path; job["done"] = time.time(); job["msg"] = "ok"
    except Exception as e:
        job["status"] = "error"; job["msg"] = repr(e)

def _cleanup_jobs():
    now = time.time()
    for t, j in list(_prep_jobs.items()):
        if j.get("done") and now - j["done"] > CLEANUP_TTL:
            try:
                if j.get("path"): os.remove(j["path"])
            except Exception:
                pass
            _prep_jobs.pop(t, None)

@app.post("/api/prepare/{vid}")
async def api_prepare_start(vid: str, request: Request):
    ip = _client_ip(request)
    if not _rate_limit_check(ip):
        raise HTTPException(429, "Demasiadas preparaciones, intenta más tarde")
    token = _token_for(vid)
    _prep_jobs[token] = {
        "vid": vid, "status": "queued", "msg": None,
        "path": None, "started": time.time(), "done": None, "ip": ip
    }
    asyncio.create_task(_run_prep_job(token, vid))
    return {"token": token}

@app.get("/api/prepare/{token}")
async def api_prepare_status(token: str):
    _cleanup_jobs()
    job = _prep_jobs.get(token)
    if not job: raise HTTPException(404, "token desconocido")
    return {
        "status": job["status"],
        "msg": job.get("msg"),
        "path": bool(job.get("path")),
        "vid": job.get("vid"),
    }

@app.get("/audio-local/{token}")
async def audio_local(token: str, range: Optional[str] = Header(None)):
    job = _prep_jobs.get(token)
    if not job or job.get("status") != "ready" or not job.get("path"):
        raise HTTPException(404, "no listo")
    path = job["path"]
    file_size = os.path.getsize(path)

    # Range
    start, end = 0, file_size - 1
    if range:
        m = re.match(r"bytes=(\d+)-(\d+)?", range or "")
        if m:
            start = int(m.group(1))
            if m.group(2): end = int(m.group(2))
            if end >= file_size: end = file_size - 1
            if start > end: raise HTTPException(416, "Range Not Satisfiable")
    length = end - start + 1

    async def iter_file(p: str, start_pos: int, bytes_left: int):
        with open(p, "rb") as f:
            f.seek(start_pos)
            chunk = 64 * 1024
            while bytes_left > 0:
                n = f.read(min(chunk, bytes_left))
                if not n: break
                bytes_left -= len(n)
                yield n

    headers = {
        "Content-Type": "audio/mp4",
        "Accept-Ranges": "bytes",
        "Cache-Control": "no-store",
        "Content-Disposition": "inline",
    }
    status_code = 200
    if range:
        headers["Content-Range"] = f"bytes {start}-{end}/{file_size}"
        status_code = 206

    return StreamingResponse(
        iter_file(path, start, length),
        status_code=status_code,
        headers=headers,
        media_type="audio/mp4",
    )

# =========================
#   Debug
# =========================

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
    return {"node": base, "status": r.status_code, "text": r.text[:2000]}

@app.get("/api/debug/pick")
async def api_debug_pick():
    base, _ = await pick_node()
    return {"node": base}
