"""FastAPI application for Safari Review Scraper web UI."""
from pathlib import Path

from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse

from .routes import router
from .websocket import manager
from .scraper_runner import scraper_runner

# Create FastAPI app
app = FastAPI(
    title="Safari Review Scraper",
    description="Web UI for scraping and analyzing safari reviews",
    version="1.0.0",
)

# Include API routes
app.include_router(router)

# Static files directory
STATIC_DIR = Path(__file__).parent / "static"
STATIC_DIR.mkdir(exist_ok=True)

# Mount static files
app.mount("/static", StaticFiles(directory=str(STATIC_DIR)), name="static")


@app.get("/")
async def root():
    """Serve the main UI."""
    index_path = STATIC_DIR / "index.html"
    if index_path.exists():
        return FileResponse(index_path)
    return {"message": "Safari Review Scraper API", "docs": "/docs"}


@app.websocket("/ws/scrape")
async def websocket_endpoint(websocket: WebSocket):
    """WebSocket endpoint for real-time scrape progress."""
    await manager.connect(websocket)

    # Send current status on connect
    await websocket.send_json({
        "type": "connected",
        "status": scraper_runner.get_status(),
    })

    try:
        while True:
            # Keep connection alive and handle any incoming messages
            data = await websocket.receive_text()

            # Handle ping/pong for keepalive
            if data == "ping":
                await websocket.send_text("pong")
            elif data == "status":
                await websocket.send_json({
                    "type": "status",
                    "status": scraper_runner.get_status(),
                })

    except WebSocketDisconnect:
        manager.disconnect(websocket)


@app.on_event("startup")
async def startup_event():
    """Initialize on startup."""
    print("Safari Review Scraper Web UI starting...")
    print(f"Static files: {STATIC_DIR}")


@app.on_event("shutdown")
async def shutdown_event():
    """Cleanup on shutdown."""
    # Stop any running scrape
    if scraper_runner.status.is_running:
        await scraper_runner.stop_scrape()

    print("Safari Review Scraper Web UI shutting down...")
