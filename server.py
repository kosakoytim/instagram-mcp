"""
Instagram MCP Server — implements MCP Streamable HTTP transport.
Token is passed via ?token= query parameter on every request.

Architecture:
  - POST /mcp?token=xxx  → JSON-RPC requests (initialize, tools/list, tools/call)
  - GET  /mcp?token=xxx  → SSE stream (for server→client notifications)
  - DELETE /mcp           → Session cleanup (no-op)

The n8n MCP Client node connects with the token in the URL query param,
and it's included on every HTTP request in the session.
"""

import json
import logging
import os
from typing import Any
from urllib.parse import urlencode, urlparse, parse_qs
from datetime import datetime, timedelta

import httpx
from fastapi import FastAPI, Request, Response, Query
from fastapi.responses import StreamingResponse, JSONResponse
from pydantic import BaseModel

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = FastAPI(title="Instagram MCP Server")

IG_API_BASE = "https://graph.facebook.com/v22.0"

# ── Server-side OAuth credentials ─────────────────────────────────────
# ONE Facebook App for the entire deployment. Each user authorizes this
# app to access their Instagram. Their per-user token is stored in their
# own Supabase schema. These server-side secrets never reach the user.

FB_APP_ID = os.environ.get("FB_APP_ID", "")
FB_APP_SECRET = os.environ.get("FB_APP_SECRET", "")
OAUTH_REDIRECT_URI = os.environ.get(
    "OAUTH_REDIRECT_URI",
    "https://n8n.timothykosakoy.com/webhook/instagram-oauth-callback",
)


# ── Instagram Graph API helper ────────────────────────────────────────

async def _ig_request(
    method: str,
    endpoint: str,
    token: str,
    params: dict | None = None,
    data: dict | None = None,
) -> dict:
    """Make a request to Instagram Graph API."""
    if params is None:
        params = {}
    params["access_token"] = token

    url = f"{IG_API_BASE}/{endpoint}"

    async with httpx.AsyncClient(timeout=httpx.Timeout(30.0)) as client:
        if method == "GET":
            resp = await client.get(url, params=params)
        elif method == "POST":
            if data:
                resp = await client.post(url, params=params, json=data)
            else:
                resp = await client.post(url, params=params)
        elif method == "DELETE":
            resp = await client.delete(url, params=params)
        else:
            raise ValueError(f"Unsupported method: {method}")

    result = resp.json()

    if "error" in result:
        error = result["error"]
        raise Exception(
            f"Instagram API error: {error.get('message', 'Unknown error')} "
            f"(code: {error.get('code')})"
        )

    return result


# ── Tool definitions ──────────────────────────────────────────────────
# Each tool: name → (description, input_schema, handler)
# Handler receives (token: str, arguments: dict) → str

TOOLS = {}


def tool(name: str, description: str):
    """Decorator to register a tool."""
    def decorator(func):
        TOOLS[name] = {
            "name": name,
            "description": description,
            "handler": func,
        }
        # Build schema from function signature
        import inspect
        sig = inspect.signature(func)
        properties = {}
        required = []
        for param_name, param in sig.parameters.items():
            if param_name == "token":
                continue
            param_type = "string"
            if param.annotation == int:
                param_type = "integer"
            elif param.annotation == dict:
                param_type = "object"
            properties[param_name] = {"type": param_type}
            if param.default == inspect.Parameter.empty:
                required.append(param_name)
            else:
                properties[param_name]["default"] = param.default

        TOOLS[name]["inputSchema"] = {
            "type": "object",
            "properties": properties,
            "required": required,
        }
        return func
    return decorator


@tool("get_profile_info", "Get Instagram business profile information including followers, bio, and account details.")
async def _get_profile_info(token: str, account_id: str) -> str:
    fields = "id,username,name,biography,website,profile_picture_url,followers_count,follows_count,media_count"
    data = await _ig_request("GET", account_id, token, params={"fields": fields})
    return json.dumps(data, indent=2)


@tool("get_media_posts", "Get recent media posts from Instagram account with engagement metrics like likes, comments.")
async def _get_media_posts(token: str, account_id: str, limit: int = 25, after: str = "") -> str:
    fields = "id,media_type,media_url,permalink,thumbnail_url,caption,timestamp,like_count,comments_count"
    params = {"fields": fields, "limit": min(limit, 100)}
    if after:
        params["after"] = after
    data = await _ig_request("GET", f"{account_id}/media", token, params=params)
    return json.dumps(data, indent=2)


@tool("get_media_insights", "Get detailed insights and analytics for a specific Instagram post (reach, likes, comments, shares, saved).")
async def _get_media_insights(token: str, media_id: str, metrics: str = "reach,likes,comments,shares,saved") -> str:
    data = await _ig_request("GET", f"{media_id}/insights", token, params={"metric": metrics})
    return json.dumps(data, indent=2)


@tool("publish_media", "Upload and publish an image or video to Instagram with caption. Provide either image_url or video_url.")
async def _publish_media(token: str, account_id: str, image_url: str = "", video_url: str = "", caption: str = "", location_id: str = "") -> str:
    if not image_url and not video_url:
        return json.dumps({"error": "Either image_url or video_url is required"})

    container_data: dict = {"caption": caption}
    if image_url:
        container_data["image_url"] = image_url
    elif video_url:
        container_data["video_url"] = video_url
        container_data["media_type"] = "VIDEO"
    if location_id:
        container_data["location_id"] = location_id

    container = await _ig_request("POST", f"{account_id}/media", token, data=container_data)
    result = await _ig_request("POST", f"{account_id}/media_publish", token, data={"creation_id": container["id"]})
    return json.dumps({"success": True, "media_id": result.get("id")}, indent=2)


@tool("get_account_pages", "Get Facebook pages connected to the account and their Instagram business accounts.")
async def _get_account_pages(token: str) -> str:
    data = await _ig_request("GET", "me/accounts", token, params={"fields": "id,name,instagram_business_account"})
    return json.dumps(data, indent=2)


@tool("get_account_insights", "Get account-level insights and analytics for Instagram business account (reach, profile views, website clicks).")
async def _get_account_insights(token: str, account_id: str, metrics: str = "reach,profile_views,website_clicks", period: str = "day") -> str:
    params = {"metric": metrics, "period": period, "metric_type": "total_value"}
    data = await _ig_request("GET", f"{account_id}/insights", token, params=params)
    return json.dumps(data, indent=2)


@tool("search_hashtag", "Search for a hashtag ID by name (without the # symbol). Returns hashtag ID for use with get_hashtag_recent_media.")
async def _search_hashtag(token: str, account_id: str, hashtag_name: str) -> str:
    params = {"q": hashtag_name, "user_id": account_id}
    data = await _ig_request("GET", "ig_hashtag_search", token, params=params)
    return json.dumps(data, indent=2)


@tool("get_hashtag_recent_media", "Get recent posts for a hashtag. Use search_hashtag first to get the hashtag_id.")
async def _get_hashtag_recent_media(token: str, account_id: str, hashtag_id: str, limit: int = 25) -> str:
    fields = "id,media_type,media_url,permalink,caption,timestamp,like_count,comments_count"
    params = {"user_id": account_id, "fields": fields, "limit": min(limit, 50)}
    data = await _ig_request("GET", f"{hashtag_id}/recent_media", token, params=params)
    return json.dumps(data, indent=2)


@tool("get_comments", "Get comments on a specific Instagram post, including replies.")
async def _get_comments(token: str, media_id: str, limit: int = 50) -> str:
    fields = "id,text,username,timestamp,like_count,replies{id,text,username,timestamp}"
    params = {"fields": fields, "limit": min(limit, 100)}
    data = await _ig_request("GET", f"{media_id}/comments", token, params=params)
    return json.dumps(data, indent=2)


@tool("reply_to_comment", "Reply to a comment on an Instagram post.")
async def _reply_to_comment(token: str, comment_id: str, message: str) -> str:
    data = await _ig_request("POST", f"{comment_id}/replies", token, data={"message": message})
    return json.dumps(data, indent=2)


@tool("get_conversations", "Get Instagram DM conversations. Requires instagram_manage_messages Advanced Access permission from Meta.")
async def _get_conversations(token: str, page_id: str, limit: int = 25) -> str:
    params = {"platform": "instagram", "fields": "id,updated_time,message_count", "limit": min(limit, 100)}
    data = await _ig_request("GET", f"{page_id}/conversations", token, params=params)
    return json.dumps(data, indent=2)


@tool("get_conversation_messages", "Get messages from a specific Instagram DM conversation. Requires Advanced Access.")
async def _get_conversation_messages(token: str, conversation_id: str, limit: int = 25) -> str:
    fields = "id,from,to,message,created_time,attachments"
    params = {"fields": f"messages{{{fields}}}", "limit": min(limit, 100)}
    data = await _ig_request("GET", conversation_id, token, params=params)
    return json.dumps(data, indent=2)


@tool("send_dm", "Send Instagram direct message. Requires Advanced Access. Can only reply within 24h of user's last message.")
async def _send_dm(token: str, recipient_id: str, message: str) -> str:
    message_data = {"recipient": {"id": recipient_id}, "message": {"text": message}}
    data = await _ig_request("POST", "me/messages", token, data=message_data)
    return json.dumps(data, indent=2)


@tool("validate_token", "Validate the Instagram/Facebook access token and check what it has access to.")
async def _validate_token(token: str) -> str:
    try:
        data = await _ig_request("GET", "me", token, params={"fields": "id,name"})
        return json.dumps({"valid": True, **data}, indent=2)
    except Exception as e:
        return json.dumps({"valid": False, "error": str(e)}, indent=2)


# ── MCP Streamable HTTP transport ─────────────────────────────────────

def _make_tool_list() -> list:
    """Build the tools/list response."""
    return [
        {
            "name": t["name"],
            "description": t["description"],
            "inputSchema": t["inputSchema"],
        }
        for t in TOOLS.values()
    ]


async def _handle_jsonrpc(rpc: dict, token: str) -> dict:
    """Handle a single JSON-RPC request."""
    method = rpc.get("method", "")
    rpc_id = rpc.get("id")
    params = rpc.get("params", {})

    if method == "initialize":
        return {
            "jsonrpc": "2.0",
            "id": rpc_id,
            "result": {
                "protocolVersion": "2025-03-26",
                "capabilities": {"tools": {}},
                "serverInfo": {
                    "name": "instagram-mcp-server",
                    "version": "1.0.0",
                },
            },
        }

    elif method == "notifications/initialized":
        # Notification — no response needed, but return empty for safety
        return {}

    elif method == "tools/list":
        return {
            "jsonrpc": "2.0",
            "id": rpc_id,
            "result": {"tools": _make_tool_list()},
        }

    elif method == "tools/call":
        tool_name = params.get("name", "")
        arguments = params.get("arguments", {})

        if tool_name not in TOOLS:
            return {
                "jsonrpc": "2.0",
                "id": rpc_id,
                "error": {"code": -32601, "message": f"Unknown tool: {tool_name}"},
            }

        handler = TOOLS[tool_name]["handler"]
        try:
            # Inject token into arguments
            call_args = {"token": token, **arguments}
            result_text = await handler(**call_args)
            return {
                "jsonrpc": "2.0",
                "id": rpc_id,
                "result": {
                    "content": [{"type": "text", "text": result_text}],
                },
            }
        except Exception as e:
            logger.error(f"Tool {tool_name} failed: {e}")
            return {
                "jsonrpc": "2.0",
                "id": rpc_id,
                "result": {
                    "content": [{"type": "text", "text": f"Error: {str(e)}"}],
                    "isError": True,
                },
            }

    elif method == "ping":
        return {"jsonrpc": "2.0", "id": rpc_id, "result": {}}

    else:
        return {
            "jsonrpc": "2.0",
            "id": rpc_id,
            "error": {"code": -32601, "message": f"Method not found: {method}"},
        }


@app.post("/mcp")
async def mcp_post(request: Request, token: str = Query(default="")):
    """Handle MCP JSON-RPC requests over POST."""
    if not token:
        return JSONResponse(
            status_code=401,
            content={"error": "Missing token query parameter"},
        )

    body = await request.json()

    # Handle batch requests
    if isinstance(body, list):
        results = []
        for rpc in body:
            result = await _handle_jsonrpc(rpc, token)
            if result:
                results.append(result)
        return JSONResponse(content=results if results else {})

    result = await _handle_jsonrpc(body, token)
    if not result:
        # Notification — return 202 Accepted
        return Response(status_code=202)
    return JSONResponse(content=result)


@app.get("/mcp")
async def mcp_get(token: str = Query(default="")):
    """SSE endpoint for server→client notifications (minimal implementation)."""
    if not token:
        return JSONResponse(
            status_code=401,
            content={"error": "Missing token query parameter"},
        )

    # Return a minimal SSE stream that just keeps the connection alive.
    # Tool results are returned synchronously via POST, so we don't need
    # to push anything over SSE for basic functionality.
    async def event_stream():
        # Initial keepalive
        yield ": connected\n\n"
        # Keep connection open with periodic heartbeats
        import asyncio
        while True:
            await asyncio.sleep(30)
            yield ": heartbeat\n\n"

    return StreamingResponse(
        event_stream(),
        media_type="text/event-stream",
        headers={"Cache-Control": "no-cache", "Connection": "keep-alive"},
    )


@app.delete("/mcp")
async def mcp_delete():
    """Session cleanup — no-op for stateless server."""
    return Response(status_code=200)


class ExchangeRequest(BaseModel):
    code: str


@app.get("/auth/url")
async def get_auth_url(state: str = ""):
    """Return the Facebook/Instagram OAuth authorization URL.

    The optional ``state`` param is forwarded to the OAuth dialog and
    survives the redirect, so the n8n webhook knows which Supabase schema
    to store the token in.
    """
    if not FB_APP_ID:
        return JSONResponse(status_code=500, content={"error": "FB_APP_ID not configured"})

    params = urlencode({
        "client_id": FB_APP_ID,
        "redirect_uri": OAUTH_REDIRECT_URI,
        "scope": ",".join([
            "instagram_basic", "instagram_manage_comments", "instagram_manage_insights",
            "instagram_manage_messages", "instagram_content_publish",
            "pages_show_list", "pages_read_engagement", "pages_manage_engagement",
            "business_management",
        ]),
        "response_type": "code",
        "state": state,
    })
    api_version = IG_API_BASE.split("/")[-1]
    return {"auth_url": f"https://www.facebook.com/{api_version}/dialog/oauth?{params}"}


@app.post("/auth/exchange")
async def exchange_code(req: ExchangeRequest):
    """Exchange an OAuth code → page token + IG business account ID.

    Called by the n8n webhook after Facebook redirects with ``?code=…``.
    """
    if not FB_APP_ID or not FB_APP_SECRET:
        return JSONResponse(status_code=500, content={"error": "OAuth creds not configured"})

    code = req.code
    if not code:
        return JSONResponse(status_code=400, content={"error": "No code provided"})

    try:
        async with httpx.AsyncClient(timeout=30.0) as c:
            # 1. Code → short-lived token
            r = await c.get(f"{IG_API_BASE}/oauth/access_token", params={
                "client_id": FB_APP_ID, "client_secret": FB_APP_SECRET,
                "redirect_uri": OAUTH_REDIRECT_URI, "code": code,
            })
            short = r.json()
            if "error" in short:
                raise Exception(short["error"].get("message", short))
            # 2. Short → long-lived
            r = await c.get(f"{IG_API_BASE}/oauth/access_token", params={
                "grant_type": "fb_exchange_token", "client_id": FB_APP_ID,
                "client_secret": FB_APP_SECRET, "fb_exchange_token": short["access_token"],
            })
            long = r.json()
            if "error" in long:
                raise Exception(long["error"].get("message", long))
            # 3. Pages → find IG business account
            r = await c.get(f"{IG_API_BASE}/me/accounts", params={
                "access_token": long["access_token"],
                "fields": "id,name,access_token,instagram_business_account",
            })
            pages = r.json().get("data", [])

        ig_page = next((p for p in pages if p.get("instagram_business_account")), None)
        if not ig_page:
            return JSONResponse(status_code=400, content={"error": "No IG business account found on any page"})

        expires_in = long.get("expires_in", 5184000)
        expires_at = (datetime.utcnow() + timedelta(seconds=expires_in)).isoformat() + "Z"
        return {
            "status": "ok",
            "access_token": ig_page["access_token"],
            "user_token": long["access_token"],
            "page_id": ig_page["id"],
            "ig_business_account_id": ig_page["instagram_business_account"]["id"],
            "expires_at": expires_at,
        }
    except Exception as e:
        logger.error(f"Exchange failed: {e}")
        return JSONResponse(status_code=400, content={"error": str(e)})


@app.post("/auth/refresh")
async def refresh_token_endpoint(req: dict):
    """Refresh a long-lived token."""
    user_token = req.get("user_token", "")
    if not user_token:
        return JSONResponse(status_code=400, content={"error": "Missing user_token"})
    try:
        async with httpx.AsyncClient(timeout=30.0) as c:
            r = await c.get(f"{IG_API_BASE}/refresh_access_token", params={
                "grant_type": "ig_refresh_token", "access_token": user_token,
            })
            refreshed = r.json()
            if "error" in refreshed:
                raise Exception(refreshed["error"].get("message", refreshed))
            r = await c.get(f"{IG_API_BASE}/me/accounts", params={
                "access_token": refreshed["access_token"],
                "fields": "id,name,access_token,instagram_business_account",
            })
            pages = r.json().get("data", [])
        ig_page = next((p for p in pages if p.get("instagram_business_account")), None)
        if not ig_page:
            raise Exception("No IG business account found")
        expires_at = (datetime.utcnow() + timedelta(seconds=refreshed.get("expires_in", 5184000))).isoformat() + "Z"
        return {"status": "ok", "access_token": ig_page["access_token"], "user_token": refreshed["access_token"], "expires_at": expires_at}
    except Exception as e:
        return JSONResponse(status_code=400, content={"error": str(e)})


@app.get("/health")
async def health():
    return {"status": "ok", "service": "instagram-mcp-server", "tools": len(TOOLS)}


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8001)