# import logging
# from starlette.middleware.base import BaseHTTPMiddleware
# from fastapi import Request

# # Configure logging
# logging.basicConfig(
#     level=logging.INFO,
#     format="%(asctime)s - %(levelname)s - %(message)s",
#     handlers=[
#         logging.FileHandler("logs/app.log"),
#         logging.StreamHandler() 
#     ],
# )

# class LoggingMiddleware(BaseHTTPMiddleware):
#     async def dispatch(self, request: Request, call_next):
#         response = await call_next(request)
#         logging.info(f"Request: {request.method} {request.url} - Response Status: {response.status_code}")
#         return response

import json
import logging
from starlette.middleware.base import BaseHTTPMiddleware
from fastapi import Request
from datetime import datetime

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(message)s",  # Let logs be raw JSON for Cloud Logging
    handlers=[
        logging.StreamHandler(),  # Cloud Run expects stdout
        logging.FileHandler("logs/app.log"),  # Local backup
    ],
)


class LoggingMiddleware(BaseHTTPMiddleware):
    async def dispatch(self, request: Request, call_next):
        # Allow preflight requests through (CORS)
        if request.method == "OPTIONS":
            return await call_next(request)

        # Record start time
        start_time = datetime.utcnow()

        try:
            # Read request body (safe read)
            body = await request.body()
            path = request.url.path

            # Skip sensitive routes
            if path not in ["/login", "/auth", "/sensitive"]:
                log_data = {
                    "event": "incoming_request",
                    "timestamp": start_time.isoformat() + "Z",
                    "method": request.method,
                    "path": path,
                    "client_host": request.client.host if request.client else None,
                }

                # Try parse JSON payload
                try:
                    parsed = json.loads(body)
                    log_data["payload"] = parsed
                except Exception:
                    if body:
                        log_data["raw_body"] = body.decode("utf-8", errors="replace")

                logging.info(json.dumps(log_data))

        except Exception as e:
            logging.warning(json.dumps({
                "event": "log_error",
                "error": str(e)
            }))

        # Get response
        response = await call_next(request)

        # Log response info
        try:
            duration_ms = (datetime.utcnow() - start_time).total_seconds() * 1000
            res_log = {
                "event": "response",
                "timestamp": datetime.utcnow().isoformat() + "Z",
                "method": request.method,
                "path": request.url.path,
                "status_code": response.status_code,
                "duration_ms": round(duration_ms, 2),
            }
            logging.info(json.dumps(res_log))
        except Exception as e:
            logging.warning(json.dumps({
                "event": "response_log_error",
                "error": str(e)
            }))

        return response
