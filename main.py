"""
Main FastAPI application for pratyayprofile_backend.

Provides:
- Health check
- CRUD endpoints for arbitrary MongoDB databases/collections
- Blog fetching endpoint that proxies Hashnode GraphQL via `mongodb_conn.getBlogs`

This file uses the helper functions defined in `mongodb_conn.py`:
- post_data
- get_data
- get_multiple_data
- data_update
- data_delete
- getBlogs
"""

import json
import os
from typing import Any, Dict, Optional

from bson import ObjectId
from fastapi import Body, FastAPI, Header, HTTPException, status

# Import repository functions
from mongodb_conn import (
    data_delete,
    data_update,
    get_data,
    get_multiple_data,
    getBlogs,
    post_data,
)

app = FastAPI(title="Pratyay Profile Backend API", version="1.0")


def serialize_doc(doc: Optional[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
    """
    Convert a MongoDB document to a JSON-serializable dict.
    Specifically, convert ObjectId to str for the `_id` field.
    """
    if doc is None:
        return None
    out = dict(doc)
    _id = out.get("_id")
    try:
        if _id is not None:
            out["_id"] = str(_id)
    except Exception:
        # if conversion fails, leave as-is (FastAPI/JSON encoder will likely fail later)
        out["_id"] = out.get("_id")
    return out


@app.get("/health", tags=["health"])
async def health():
    """
    Simple health check.
    """
    return {"status": "ok"}


@app.post("/message", status_code=status.HTTP_201_CREATED)
async def create_data(
    database: str,
    collection: str,
    payload: Dict[str, Any] = Body(...),
    x_password: Optional[str] = Header(None, alias="X-Password"),
):
    """
    Insert a document into the given database and collection.
    Body: arbitrary JSON object to insert.
    Returns the inserted document id.
    """
    try:
        # Restrict create for users collection unless admin password matches
        admin_pass = os.getenv("ADMIN_PASS")
        if not admin_pass or x_password != admin_pass:
            raise HTTPException(
                status_code=403,
                detail="Forbidden: invalid admin password for users collection",
            )
        result = await post_data(database, collection, payload)
        inserted_id = getattr(result, "inserted_id", None)
        return {"inserted_id": str(inserted_id) if inserted_id is not None else None}
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to insert data: {e}")


@app.get("/data", tags=["data"])
async def list_data(
    database: str,
    collection: str,
    q: Optional[str] = None,
    limit: Optional[int] = None,
):
    """
    List documents from a collection.
    - q: optional JSON-encoded query string (e.g. '{"name": "Alice"}')
    - limit: optional integer to limit number of results
    """
    try:
        query = {}
        if q:
            try:
                query = json.loads(q)
                if not isinstance(query, dict):
                    raise ValueError("Query must be a JSON object")
            except json.JSONDecodeError as je:
                raise HTTPException(
                    status_code=400, detail=f"Invalid JSON for query: {je}"
                )
        if limit is None:
            results = await get_multiple_data(database, collection, query)
        else:
            results = await get_multiple_data(database, collection, query, int(limit))
        return [serialize_doc(r) for r in results]
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to list data: {e}")


@app.get("/data/{database}/{collection}/{id}", tags=["data"])
async def get_document(database: str, collection: str, id: str):
    """
    Get a single document by its ObjectId.
    """
    try:
        oid = ObjectId(id)
    except Exception:
        raise HTTPException(status_code=400, detail="Invalid ObjectId format")
    try:
        result = await get_data(database, collection, {"_id": oid})
        doc = serialize_doc(result)
        if not doc:
            raise HTTPException(status_code=404, detail="Document not found")
        return doc
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to fetch document: {e}")


@app.put("/data/{database}/{collection}/{id}", tags=["data"])
async def update_document(
    database: str,
    collection: str,
    id: str,
    payload: Dict[str, Any] = Body(...),
    x_password: Optional[str] = Header(None, alias="X-Password"),
):
    """
    Update fields of an existing document by ObjectId.
    Body: fields to set (partial update).
    Returns modified and matched counts.
    """
    try:
        oid = ObjectId(id)
    except Exception:
        raise HTTPException(status_code=400, detail="Invalid ObjectId format")
    try:
        # Restrict update for users collection unless admin password matches
        admin_pass = os.getenv("ADMIN_PASS")
        if not admin_pass or x_password != admin_pass:
            raise HTTPException(
                status_code=403,
                detail="Forbidden: invalid admin password for users collection",
            )
        result = await data_update(
            database, collection, {"_id": oid}, {"$set": payload}
        )
        # result is UpdateResult
        matched = getattr(result, "matched_count", None)
        modified = getattr(result, "modified_count", None)
        return {"matched_count": matched, "modified_count": modified}
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to update document: {e}")


@app.delete("/data/{database}/{collection}/{id}", tags=["data"])
async def delete_document(
    database: str,
    collection: str,
    id: str,
    x_password: Optional[str] = Header(None, alias="X-Password"),
):
    """
    Delete a single document by ObjectId.
    Returns deleted count.
    """
    try:
        oid = ObjectId(id)
    except Exception:
        raise HTTPException(status_code=400, detail="Invalid ObjectId format")
    try:
        # Restrict delete for users collection unless admin password matches
        admin_pass = os.getenv("ADMIN_PASS")
        if not admin_pass or x_password != admin_pass:
            raise HTTPException(
                status_code=403,
                detail="Forbidden: invalid admin password for users collection",
            )
        result = await data_delete(database, collection, {"_id": oid})
        deleted = getattr(result, "deleted_count", None)
        return {"deleted_count": deleted}
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to delete document: {e}")


@app.get("/blogs", tags=["blogs"])
async def get_blogs(num: int = 10):
    """
    Fetch recent blog posts from a configured Hashnode publication.
    - num: number of posts to fetch (default 10)
    """
    try:
        posts = await getBlogs(num)
        sanitized = []
        for edge in posts:
            node = edge.get("node") if isinstance(edge, dict) else None
            if node:
                # no ObjectId conversion expected here; leave as returned
                sanitized.append(node)
        return sanitized
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to fetch blogs: {e}")


# Header-based CRUD endpoints
# These endpoints read database, collection, and id from request headers:
# - X-Database
# - X-Collection
# - X-Id (where applicable)
# Optional headers:
# - X-Query: JSON-encoded query string
# - X-Limit: integer limit


@app.post("/data/headers", status_code=status.HTTP_201_CREATED, tags=["data"])
async def create_data_headers(
    x_database: str = Header(..., alias="X-Database"),
    x_collection: str = Header(..., alias="X-Collection"),
    payload: Dict[str, Any] = Body(...),
    x_password: Optional[str] = Header(None, alias="X-Password"),
):
    """
    Insert a document using headers for database and collection.
    Headers:
    - X-Database
    - X-Collection
    Body: arbitrary JSON object to insert.
    """
    try:
        # Restrict create for users collection unless admin password matches
        admin_pass = os.getenv("ADMIN_PASS")
        if not admin_pass or x_password != admin_pass:
            raise HTTPException(
                status_code=403,
                detail="Forbidden: invalid admin password for users collection",
            )
        result = await post_data(x_database, x_collection, payload)
        inserted_id = getattr(result, "inserted_id", None)
        return {"inserted_id": str(inserted_id) if inserted_id is not None else None}
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to insert data: {e}")


@app.get("/data/headers", tags=["data"])
async def list_data_headers(
    x_database: str = Header(..., alias="X-Database"),
    x_collection: str = Header(..., alias="X-Collection"),
    x_query: Optional[str] = Header(None, alias="X-Query"),
    x_limit: Optional[int] = Header(None, alias="X-Limit"),
):
    """
    List documents using headers for database and collection.
    Headers:
    - X-Database
    - X-Collection
    Optional:
    - X-Query: JSON-encoded query string
    - X-Limit: integer limit
    """
    try:
        query = {}
        if x_query:
            try:
                query = json.loads(x_query)
                if not isinstance(query, dict):
                    raise ValueError("Query must be a JSON object")
            except json.JSONDecodeError as je:
                raise HTTPException(
                    status_code=400, detail=f"Invalid JSON for query: {je}"
                )
        if x_limit is None:
            results = await get_multiple_data(x_database, x_collection, query)
        else:
            results = await get_multiple_data(
                x_database, x_collection, query, int(x_limit)
            )
        return [serialize_doc(r) for r in results]
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to list data: {e}")


@app.get("/data/headers/document", tags=["data"])
async def get_document_headers(
    x_database: str = Header(..., alias="X-Database"),
    x_collection: str = Header(..., alias="X-Collection"),
    x_id: str = Header(..., alias="X-Id"),
):
    """
    Get a single document by ObjectId using headers.
    Headers:
    - X-Database
    - X-Collection
    - X-Id
    """
    try:
        oid = ObjectId(x_id)
    except Exception:
        raise HTTPException(status_code=400, detail="Invalid ObjectId format")
    try:
        result = await get_data(x_database, x_collection, {"_id": oid})
        doc = serialize_doc(result)
        if not doc:
            raise HTTPException(status_code=404, detail="Document not found")
        return doc
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to fetch document: {e}")


@app.put("/data/headers/document", tags=["data"])
async def update_document_headers(
    x_database: str = Header(..., alias="X-Database"),
    x_collection: str = Header(..., alias="X-Collection"),
    x_id: str = Header(..., alias="X-Id"),
    payload: Dict[str, Any] = Body(...),
    x_password: Optional[str] = Header(None, alias="X-Password"),
):
    """
    Update fields of an existing document by ObjectId using headers.
    Headers:
    - X-Database
    - X-Collection
    - X-Id
    Body: fields to set (partial update).
    """
    try:
        oid = ObjectId(x_id)
    except Exception:
        raise HTTPException(status_code=400, detail="Invalid ObjectId format")
    try:
        # Restrict update for users collection unless admin password matches
        admin_pass = os.getenv("ADMIN_PASS")
        if not admin_pass or x_password != admin_pass:
            raise HTTPException(
                status_code=403,
                detail="Forbidden: invalid admin password for users collection",
            )
        result = await data_update(
            x_database, x_collection, {"_id": oid}, {"$set": payload}
        )
        matched = getattr(result, "matched_count", None)
        modified = getattr(result, "modified_count", None)
        return {"matched_count": matched, "modified_count": modified}
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to update document: {e}")


@app.delete("/data/headers/document", tags=["data"])
async def delete_document_headers(
    x_database: str = Header(..., alias="X-Database"),
    x_collection: str = Header(..., alias="X-Collection"),
    x_id: str = Header(..., alias="X-Id"),
    x_password: Optional[str] = Header(None, alias="X-Password"),
):
    """
    Delete a single document by ObjectId using headers.
    Headers:
    - X-Database
    - X-Collection
    - X-Id
    """
    try:
        oid = ObjectId(x_id)
    except Exception:
        raise HTTPException(status_code=400, detail="Invalid ObjectId format")
    try:
        # Restrict delete for users collection unless admin password matches
        admin_pass = os.getenv("ADMIN_PASS")
        if not admin_pass or x_password != admin_pass:
            raise HTTPException(
                status_code=403,
                detail="Forbidden: invalid admin password for users collection",
            )
        result = await data_delete(x_database, x_collection, {"_id": oid})
        deleted = getattr(result, "deleted_count", None)
        return {"deleted_count": deleted}
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to delete document: {e}")
