import os
import time
import random
import re
import shutil
import tempfile
from typing import List, Tuple, Optional, Dict, Any
import asyncio
from datetime import datetime
from anyio import EndOfStream
import anyio

import httpx
from starlette.responses import StreamingResponse
from fastapi import FastAPI, Request, HTTPException, Header
from fastapi.responses import HTMLResponse, RedirectResponse, StreamingResponse, Response, FileResponse
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

# Cache salud nodos
_health_cache: Dict[str, Tuple[bool, float]] = {}  # base -> (ok, ts)

async def _list_candidates(vid: str) -> List[str]:
    """Devuelve varias URLs candidatas de Piped e Invidious."""
    urls: List[str] = []

    # 1) Piped (audioStreams directos)
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

                # prioriza M4A/AAC; si no hay, el mayor bitrate (suele ser webm/opus)
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

    # 2) Invidious latest_version (forzamos proxy local=true) con itags típicos
    for base in INVIDIOUS_INSTANCES:
        for it in [140, 251, 171, 250, 249]:
            urls.append(f"{base}/latest_version?id={vid}&itag={it}&local=true")

    # quitar duplicados manteniendo orden
    seen = set()
    uniq = []
    for u in urls:
        if u in seen:
            continue
        seen.add(u)
        uniq.append(u)
    return uniq


async def _open_stream_validated(url: str, range_header: Optional[str]) -> Tuple[httpx.Response, httpx.AsyncClient]:
    """
    Abre el stream real (sin sonda previa). http2=False para estabilidad.
    Devuelve (response_stream, client). Quien llama debe cerrar ambos.
    """
    client = httpx.AsyncClient(follow_redirects=True, headers=_UA, timeout=None, http2=False)

    headers = {}
    if range_header:
        headers["Range"] = range_header

    try:
        req = client.build_request("GET", url, headers=headers)
        resp = await client.send(req, stream=True)  # <- clave: send(..., stream=True)
        if resp.status_code >= 400:
            await resp.aclose()
            await client.aclose()
            raise HTTPException(502, f"upstream {resp.status_code}")
        return resp, client
    except Exception:
        # si algo falla, cerramos el client
        try:
            await client.aclose()
        except Exception:
            pass
        raise


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
#   Resolución audio público
# =========================

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

_resolve_cache: Dict[str, Tuple[float, str]] = {}  # vid -> (exp_ts, url)
_UA = {"User-Agent": "Mozilla/5.0 (compatible; lavalui/1.0)"}
_ITAGS = [140, 251, 171, 250, 249]

async def _try_piped(client: httpx.AsyncClient, base: str, vid: str) -> Optional[str]:
    try:
        r = await client.get(f"{base}/streams/{vid}", headers=_UA, timeout=RESOLVE_TIMEOUT)
        if r.status_code != 200:
            return None
        js = r.json()
        streams = js.get("audioStreams") or []
        if not streams:
            return None
        best = max(streams, key=lambda s: s.get("bitrate", 0) or 0)
        url = best.get("url")
        return url
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
app.mount("/static", StaticFiles(directory="app/static"), name="static")
templates = Jinja2Templates(directory="app/templates")

@app.get("/", response_class=HTMLResponse)
async def index(request: Request):
    return templates.TemplateResponse("index.html", {"request": request})

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
                # vid solo si realmente es vídeo de YouTube (evita playlists PL..., canales, etc.)
                vid = None
                if info.get("sourceName") == "youtube":
                    ident = info.get("identifier") or ""
                    if len(ident) == 11:  # típico videoId
                        vid = ident

                final.append({
                    "display": cand.get("display"),
                    "encoded": t.get("encoded"),  # por si luego quieres usar Lavalink directo
                    "info": {
                        "title": info.get("title"),
                        "author": info.get("author"),
                        "length": info.get("length"),
                        "uri": info.get("uri"),
                        "sourceName": info.get("sourceName"),
                        "artworkUrl": info.get("artworkUrl"),
                    },
                    # portada candidata extra (Deezer ya traía cover)
                    "cover": cand.get("cover"),
                    "vid": vid,
                })
        except Exception:
            continue

    return {"results": final}
# =========================
#   PROXY de audio (passthrough con Range) + fallback HLS
# =========================

RANGE_RE = re.compile(r"bytes=(\d+)-(\d+)?")

def _filter_hop_by_hop(headers: Dict[str, str]) -> Dict[str, str]:
    # Elimina cabeceras que no deben reenviarse
    hop = {
        "connection","keep-alive","proxy-authenticate","proxy-authorization",
        "te","trailers","transfer-encoding","upgrade"
    }
    return {k: v for k, v in headers.items() if k.lower() not in hop}

async def _proxy_stream(origin_url: str, client_range: Optional[str]) -> Response:
    async with httpx.AsyncClient(follow_redirects=True) as client:
        # Cabeceras hacia origen
        req_headers = {"User-Agent": _UA["User-Agent"], "Accept": "*/*"}
        if client_range:
            req_headers["Range"] = client_range  # ej. "bytes=0-"

        r = await client.get(origin_url, headers=req_headers, timeout=RESOLVE_TIMEOUT)

        status = r.status_code

        # Copiamos cabeceras útiles
        out_headers: Dict[str, str] = {}
        for k in [
            "Content-Type", "Content-Length", "Content-Range",
            "Accept-Ranges", "Cache-Control", "ETag", "Last-Modified"
        ]:
            if k in r.headers:
                out_headers[k] = r.headers[k]

        # Aseguramos Accept-Ranges aunque el origen no lo ponga
        out_headers.setdefault("Accept-Ranges", "bytes")

        # media_type correcto (evita que FastAPI meta text/html por defecto)
        media_type = out_headers.get("Content-Type", "application/octet-stream")

        async def gen():
            async for chunk in r.aiter_bytes(chunk_size=64 * 1024):
                yield chunk

        return StreamingResponse(
            gen(),
            status_code=status,                # 200 o 206 según el origen
            headers=_filter_hop_by_hop(out_headers),
            media_type=media_type
        )

async def _open_upstream(url: str, range_header: Optional[str]) -> httpx.Response:
    """
    Abre una conexión streaming al origen, probando primero con Range (si viene),
    y si falla, sin Range. Devuelve la respuesta httpx.Response abierta (stream=True).
    Lanza excepción si ambas fallan.
    """
    headers = {"User-Agent": _UA["User-Agent"], "Accept": "*/*"}
    client = httpx.AsyncClient(follow_redirects=True, timeout=None)

    async def _do_get(with_range: bool):
        h = headers.copy()
        if with_range and range_header:
            h["Range"] = range_header
        # stream=True para no cargar todo en memoria
        return await client.get(url, headers=h, stream=True)

    try:
        # 1) Con Range (si hay)
        resp = await _do_get(with_range=True)
        if resp.status_code < 400:
            return resp, client
        # 2) Sin Range
        await resp.aclose()
        resp = await _do_get(with_range=False)
        if resp.status_code < 400:
            return resp, client
        # Si sigue mal, error
        text = ""
        try:
            text = await resp.aread()
            text = text.decode("utf-8", "ignore")[:200]
        except Exception:
            pass
        await resp.aclose()
        await client.aclose()
        raise HTTPException(502, f"Origen respondió {resp.status_code}: {text or 'sin cuerpo'}")
    except Exception as e:
        # Cierra client si hubo excepción
        try:
            await client.aclose()
        except Exception:
            pass
        raise

@app.get("/audio-proxy/{vid}")
async def audio_proxy(vid: str, request: Request):
    """
    Proxy de audio con reintentos y soporte Range.
    - Si el upstream no devuelve Content-Range, reintentamos sin Range y entregamos 200.
    - Forzamos Content-Type de audio si viene vacío/incorrecto (p. ej. text/html).
    - No enviamos Content-Length (evita errores de longitud con streaming).
    """
    range_header = request.headers.get("range")
    candidates = await _list_candidates(vid)
    if not candidates:
        raise HTTPException(404, f"No hay fuentes públicas para {vid}")

    last_err = None

    for url in candidates[:10]:
        try:
            # 1) Intento respetando Range del cliente (si existe)
            resp, client = await _open_stream_validated(url, range_header)
        except HTTPException as e:
            last_err = e
            continue
        except Exception as e:
            last_err = HTTPException(502, repr(e))
            continue

        # Si el cliente pidió Range pero el upstream NO respondió con Content-Range,
        # reabrimos SIN Range para entregar 200 (muchos players lo aceptan).
        if range_header and not resp.headers.get("content-range"):
            try:
                await resp.aclose()
                await client.aclose()
            except Exception:
                pass
            try:
                resp, client = await _open_stream_validated(url, None)
            except Exception as e:
                last_err = HTTPException(502, f"no soporta Range y fallo sin Range: {e!r}")
                continue

        # --- Cabeceras hacia el cliente ---
        # Content-Type del upstream (si es inválido/ausente, forzamos audio)
        upstream_ct = (resp.headers.get("content-type") or "").lower()
        ct = upstream_ct if (upstream_ct and not upstream_ct.startswith("text/")) else "audio/webm"

        # Heurística: si la URL trae itag=140 o parece m4a, fuerza MP4 para máxima compatibilidad
        low_url = url.lower()
        if "itag=140" in low_url or "audio/mp4" in upstream_ct or low_url.endswith(".m4a"):
            ct = "audio/mp4"

        # ¿206 o 200?
        status = 206 if resp.headers.get("content-range") else 200

        # Construimos headers mínimos (nada de Content-Length)
        out_headers = {
            "accept-ranges": "bytes",
            "cache-control": "no-store",
            "content-type": ct,
            # este header ayuda a desactivar bufferizaciones en algunos reverse proxies
            "x-accel-buffering": "no",
            # aconseja inline en navegador
            "content-disposition": "inline"
        }
        # Propagamos Content-Range si existe
        if resp.headers.get("content-range"):
            out_headers["content-range"] = resp.headers["content-range"]

        async def body_iter():
            try:
                async for chunk in resp.aiter_bytes(64 * 1024):
                    if await request.is_disconnected():
                        break
                    yield chunk
            finally:
                try:
                    await resp.aclose()
                finally:
                    await client.aclose()

        # ¡OJO!: además de poner el header, pasamos media_type para que Starlette no meta text/html
        return StreamingResponse(body_iter(), status_code=status, headers=out_headers, media_type=ct)

    if last_err:
        raise last_err
    raise HTTPException(502, "No se pudo abrir ninguna fuente de audio")


# =========================
#   HLS Fallback (FFmpeg → .m3u8)
# =========================

# Carpeta temporal raíz para HLS
HLS_ROOT = "/tmp/hls_lavalui"
os.makedirs(HLS_ROOT, exist_ok=True)

# token -> {"dir":..., "expires": ts}
_hls_index: Dict[str, Dict[str, Any]] = {}

async def _ensure_hls_build(vid: str) -> Tuple[str, str]:
    """
    Asegura que existan los ficheros HLS para vid.
    Devuelve (token, playlist_path_rel).
    """
    # Reutiliza token si existe y no ha expirado
    for token, meta in list(_hls_index.items()):
        if meta.get("vid") == vid and meta["expires"] > time.time():
            return token, f"/hls/{token}/index.m3u8"

    # Resuelve URL origen
    origin = await resolve_public_audio_url(vid)
    if not origin:
        raise HTTPException(404, f"No se pudo resolver audio para {vid} (Piped/Invidious)")

    # Nuevo token/carpeta
    token = f"{vid}-{int(time.time())}-{random.randint(1000,9999)}"
    out_dir = os.path.join(HLS_ROOT, token)
    os.makedirs(out_dir, exist_ok=True)

    # Comando FFmpeg (transcodifica a AAC para máxima compatibilidad)
    # Latencia aproximada: ~2-6s con hls_time=4
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

    # Lanza FFmpeg en background (no esperamos a que termine; el player irá leyendo)
    proc = await asyncio.create_subprocess_exec(*cmd)
    # Guardamos metadatos con un TTL
    _hls_index[token] = {"dir": out_dir, "expires": time.time() + 3600, "pid": proc.pid, "vid": vid}

    return token, f"/hls/{token}/index.m3u8"

@app.get("/audio-hls/{vid}")
async def audio_hls_entry(vid: str):
    """
    Devuelve un redirect a la playlist HLS (generando si no existe).
    Usa esto como fallback desde el frontend (con hls.js).
    """
    token, rel = await _ensure_hls_build(vid)
    return RedirectResponse(url=rel, status_code=302)

@app.get("/hls/{token}/{fname}")
async def serve_hls(token: str, fname: str):
    """
    Sirve ficheros HLS generados. Seguridad básica por token y whitelisting.
    """
    meta = _hls_index.get(token)
    if not meta or meta["expires"] < time.time():
        raise HTTPException(404, "HLS expirado")
    # whitelist simple
    if not re.match(r"^(index\.m3u8|seg\d{3}\.ts)$", fname):
        raise HTTPException(400, "Nombre de fichero inválido")

    path = os.path.join(meta["dir"], fname)
    if not os.path.isfile(path):
        # Puede tardar un pelín en aparecer el primer índice
        raise HTTPException(404, "HLS no listo")
    # MIME correcto
    if fname.endswith(".m3u8"):
        return FileResponse(path, media_type="application/vnd.apple.mpegurl")
    return FileResponse(path, media_type="video/MP2T")

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

@app.get("/audio/{vid}")
async def audio_redirect(vid: str):
    origin = await resolve_public_audio_url(vid)
    if not origin:
        raise HTTPException(404, f"No se pudo resolver audio para {vid}")
    return RedirectResponse(url=origin, status_code=302)
