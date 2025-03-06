from fastapi import WebSocket, WebSocketDisconnect, Depends
from typing import Dict, Set, Optional
import json
from jose import JWTError, jwt
from datetime import datetime

class ConnectionManager:
    def __init__(self):
        self.active_connections: Dict[str, Set[WebSocket]] = {}

    async def connect(self, websocket: WebSocket, user_id: str):
        await websocket.accept()
        if user_id not in self.active_connections:
            self.active_connections[user_id] = set()
        self.active_connections[user_id].add(websocket)

    def disconnect(self, websocket: WebSocket, user_id: str):
        if user_id in self.active_connections:
            self.active_connections[user_id].discard(websocket)
            if not self.active_connections[user_id]:
                del self.active_connections[user_id]

    async def send_personal_message(self, message: dict, user_id: str):
        if user_id in self.active_connections:
            disconnected = set()
            for connection in self.active_connections[user_id]:
                try:
                    await connection.send_json(message)
                except WebSocketDisconnect:
                    disconnected.add(connection)
            
            # Clean up disconnected websockets
            for connection in disconnected:
                self.active_connections[user_id].discard(connection)
            
            if not self.active_connections[user_id]:
                del self.active_connections[user_id]

    async def broadcast(self, message: dict):
        for user_id in list(self.active_connections.keys()):
            await self.send_personal_message(message, user_id)

manager = ConnectionManager()

async def get_current_user_from_token(token: str) -> Optional[str]:
    try:
        # Use the same secret key and algorithm as your main authentication
        SECRET_KEY = "your-secret-key"  # Use the same secret key as in your main app
        ALGORITHM = "HS256"
        
        payload = jwt.decode(token, SECRET_KEY, algorithms=[ALGORITHM])
        user_id: str = payload.get("sub")
        if user_id is None:
            return None
        return user_id
    except JWTError:
        return None

async def notify_processing_started(user_id: str, filename: str):
    await manager.send_personal_message({
        "type": "processing_started",
        "timestamp": datetime.now().isoformat(),
        "filename": filename
    }, user_id)

async def notify_processing_complete(user_id: str, filename: str, stats: dict):
    await manager.send_personal_message({
        "type": "processing_complete",
        "timestamp": datetime.now().isoformat(),
        "filename": filename,
        "stats": stats
    }, user_id)

async def notify_stats_update(user_id: str, stats: dict):
    await manager.send_personal_message({
        "type": "stats_update",
        "timestamp": datetime.now().isoformat(),
        "stats": stats
    }, user_id)
