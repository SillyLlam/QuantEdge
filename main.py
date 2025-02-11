from fastapi import FastAPI, Request, HTTPException
from fastapi.templating import Jinja2Templates
from fastapi.staticfiles import StaticFiles
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import HTMLResponse, JSONResponse
import uvicorn
import json
import os
import logging

# Set up logging
logging.basicConfig(level=logging.INFO,
                   format='%(asctime)s - %(message)s',
                   datefmt='%Y-%m-%d %H:%M:%S')

# Initialize FastAPI app
app = FastAPI(title="CSV Tokenization System")

# Mount templates and static files
templates = Jinja2Templates(directory="templates")
app.mount("/static", StaticFiles(directory="static"), name="static")

# CORS middleware configuration
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Create data directories
os.makedirs("data/input", exist_ok=True)
os.makedirs("data/processed", exist_ok=True)
os.makedirs("data/archive", exist_ok=True)

# Error handler
@app.exception_handler(Exception)
async def global_exception_handler(request: Request, exc: Exception):
    logging.error(f"Error handling request: {exc}")
    return JSONResponse(
        status_code=500,
        content={"detail": "Internal server error", "error": str(exc)}
    )

# Routes
@app.get("/stats")
async def get_stats():
    """Get tokenization statistics"""
    try:
        with open("data/stats.json", 'r') as f:
            stats = json.load(f)
        return stats
    except FileNotFoundError:
        return {"total_files": 0, "total_records": 0, "sensitive_fields_found": {}}
    except Exception as e:
        logging.error(f"Error reading stats: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/token-mappings")
async def get_token_mappings():
    """Get token mappings"""
    try:
        with open("data/token_mappings.json", 'r') as f:
            mappings = json.load(f)
        return mappings
    except FileNotFoundError:
        return {}
    except Exception as e:
        logging.error(f"Error reading token mappings: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/processed-files")
async def get_processed_files():
    """Get list of processed files"""
    try:
        files = os.listdir("data/processed")
        return {"files": files}
    except FileNotFoundError:
        return {"files": []}
    except Exception as e:
        logging.error(f"Error reading processed files: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/", response_class=HTMLResponse)
async def read_root(request: Request):
    try:
        return templates.TemplateResponse("index.html", {"request": request})
    except Exception as e:
        logging.error(f"Error rendering template: {e}")
        raise HTTPException(status_code=500, detail=str(e))

if __name__ == "__main__":
    uvicorn.run("main:app", host="0.0.0.0", port=8000, reload=True)
