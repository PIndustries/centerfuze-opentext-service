"""
Database client using NATS to communicate with centerfuze-mongodb-service
"""

import json
import logging
from typing import Dict, Any, Optional, List
from datetime import datetime

logger = logging.getLogger(__name__)


class DatabaseClient:
    """Client for database operations via NATS"""
    
    def __init__(self, nc):
        self.nc = nc
        self.timeout = 30.0  # 30 second timeout for database operations
        
    async def find_one(self, collection: str, query: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """Find a single document"""
        try:
            response = await self.nc.request(
                "db.findOne",
                json.dumps({
                    "collection": collection,
                    "query": query
                }).encode(),
                timeout=self.timeout
            )
            
            result = json.loads(response.data.decode())
            if result.get("success"):
                return result.get("data", {}).get("document")
            else:
                logger.error(f"Database error: {result.get('error')}")
                return None
                
        except Exception as e:
            logger.error(f"Error finding document: {e}")
            return None
            
    async def find(self, collection: str, query: Dict[str, Any], 
                   limit: int = 100, skip: int = 0, 
                   sort: Optional[List[Tuple[str, int]]] = None) -> List[Dict[str, Any]]:
        """Find multiple documents"""
        try:
            request_data = {
                "collection": collection,
                "query": query,
                "limit": limit,
                "skip": skip
            }
            
            if sort:
                request_data["sort"] = {k: v for k, v in sort}
                
            response = await self.nc.request(
                "db.find",
                json.dumps(request_data).encode(),
                timeout=self.timeout
            )
            
            result = json.loads(response.data.decode())
            if result.get("success"):
                return result.get("data", {}).get("documents", [])
            else:
                logger.error(f"Database error: {result.get('error')}")
                return []
                
        except Exception as e:
            logger.error(f"Error finding documents: {e}")
            return []
            
    async def insert_one(self, collection: str, document: Dict[str, Any]) -> Optional[str]:
        """Insert a single document"""
        try:
            # Ensure datetime objects are serialized
            doc_copy = self._serialize_document(document)
            
            response = await self.nc.request(
                "db.insert",
                json.dumps({
                    "collection": collection,
                    "document": doc_copy
                }).encode(),
                timeout=self.timeout
            )
            
            result = json.loads(response.data.decode())
            if result.get("success"):
                return result.get("data", {}).get("inserted_id")
            else:
                logger.error(f"Database error: {result.get('error')}")
                return None
                
        except Exception as e:
            logger.error(f"Error inserting document: {e}")
            return None
            
    async def update_one(self, collection: str, query: Dict[str, Any], 
                        update: Dict[str, Any], upsert: bool = False) -> Dict[str, Any]:
        """Update a single document"""
        try:
            # Serialize update document
            update_copy = self._serialize_document(update)
            
            response = await self.nc.request(
                "db.update",
                json.dumps({
                    "collection": collection,
                    "query": query,
                    "update": update_copy,
                    "upsert": upsert,
                    "multi": False
                }).encode(),
                timeout=self.timeout
            )
            
            result = json.loads(response.data.decode())
            if result.get("success"):
                return result.get("data", {})
            else:
                logger.error(f"Database error: {result.get('error')}")
                return {"modified_count": 0}
                
        except Exception as e:
            logger.error(f"Error updating document: {e}")
            return {"modified_count": 0}
            
    async def delete_one(self, collection: str, query: Dict[str, Any]) -> Dict[str, Any]:
        """Delete a single document"""
        try:
            response = await self.nc.request(
                "db.delete",
                json.dumps({
                    "collection": collection,
                    "query": query,
                    "multi": False
                }).encode(),
                timeout=self.timeout
            )
            
            result = json.loads(response.data.decode())
            if result.get("success"):
                return result.get("data", {})
            else:
                logger.error(f"Database error: {result.get('error')}")
                return {"deleted_count": 0}
                
        except Exception as e:
            logger.error(f"Error deleting document: {e}")
            return {"deleted_count": 0}
            
    async def count_documents(self, collection: str, query: Dict[str, Any]) -> int:
        """Count documents matching query"""
        try:
            response = await self.nc.request(
                "db.count",
                json.dumps({
                    "collection": collection,
                    "query": query
                }).encode(),
                timeout=self.timeout
            )
            
            result = json.loads(response.data.decode())
            if result.get("success"):
                return result.get("data", {}).get("count", 0)
            else:
                logger.error(f"Database error: {result.get('error')}")
                return 0
                
        except Exception as e:
            logger.error(f"Error counting documents: {e}")
            return 0
            
    async def aggregate(self, collection: str, pipeline: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Run aggregation pipeline"""
        try:
            response = await self.nc.request(
                "db.aggregate",
                json.dumps({
                    "collection": collection,
                    "pipeline": pipeline
                }).encode(),
                timeout=self.timeout
            )
            
            result = json.loads(response.data.decode())
            if result.get("success"):
                return result.get("data", {}).get("documents", [])
            else:
                logger.error(f"Database error: {result.get('error')}")
                return []
                
        except Exception as e:
            logger.error(f"Error running aggregation: {e}")
            return []
            
    def _serialize_document(self, doc: Dict[str, Any]) -> Dict[str, Any]:
        """Serialize document for JSON transmission"""
        doc_copy = {}
        for key, value in doc.items():
            if isinstance(value, datetime):
                doc_copy[key] = value.isoformat()
            elif isinstance(value, dict):
                doc_copy[key] = self._serialize_document(value)
            elif isinstance(value, list):
                doc_copy[key] = [
                    self._serialize_document(item) if isinstance(item, dict) else
                    item.isoformat() if isinstance(item, datetime) else item
                    for item in value
                ]
            else:
                doc_copy[key] = value
        return doc_copy


class DatabaseManager:
    """Database manager using NATS client"""
    
    def __init__(self, nc):
        self.client = DatabaseClient(nc)
        
    def get_database(self):
        """Get database client"""
        return self.client
        
    async def disconnect(self):
        """Disconnect (no-op for NATS-based client)"""
        pass
