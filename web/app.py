import subprocess, urllib.parse
from fastapi import FastAPI, Query, HTTPException
from fastapi.responses import StreamingResponse, FileResponse
from fastapi.staticfiles import StaticFiles
import wavelink
import os

LAVA_URL = os.getenv("LAVA_URL", "http://lavalink:2333")
LAVA_PASS = os.getenv("LAVA_PASS", "youshallnotpass")

app = FastAPI()
app.mount("/static", StaticFiles(directory="/app/static"), name="static")

node: wavelink.Node | None = None

@app.on_event("startup")
async def startup():
    global node
    node = wavelink.Node(
        uri=LAVA_URL,
        password=LAVA_PASS,
        client_name="py-spot/0.1",
        user_id=1
    )
    await wavelink.NodePool.connect(nodes=[node])

@app.get("/")
def index():
    return FileResponse("/app/static/index.html")

@app.get("/api/search")
async def api_search(q: str = Query(..., min_length=2)):
    if node is None:
        raise HTTPException(503, "Lavalink no disponible")
    res = await node.search(f"ytsearch:{q}")
    tracks = [{
        "title": t.title,
        "author": getattr(t, "author", ""),
        "length": getattr(t, "length", 0),
        "uri": t.uri
    } for t in res.tracks[:12]]
    return {"tracks": tracks}

@app.get("/api/stream")
def api_stream(uri: str = Query(...)):
    # Streaming puro: no escribe a disco
    safe = urllib.parse.unquote(uri)
    ytdlp = [
        "yt-dlp",
        "--no-cache-dir", "--no-part", "--no-keep-fragments",
        "-f", "bestaudio",
        "-o", "-", "--", safe
    ]
    ff = [
        "ffmpeg", "-hide_banner", "-loglevel", "error",
        "-i", "pipe:0", "-f", "mp3", "-vn", "-b:a", "192k", "-"
    ]

    p1 = subprocess.Popen(ytdlp, stdout=subprocess.PIPE)
    p2 = subprocess.Popen(ff, stdin=p1.stdout, stdout=subprocess.PIPE)
    p1.stdout.close()  # para que ffmpeg detecte fin

    if not p2.stdout:
        # limpieza básica
        try: p2.terminate()
        except: pass
        try: p1.terminate()
        except: pass
        raise HTTPException(500, "Stream no disponible")

    # Cache-Control no-store: todo es efímero
    return StreamingResponse(
        p2.stdout,
        media_type="audio/mpeg",
        headers={"Cache-Control": "no-store"}
    )
