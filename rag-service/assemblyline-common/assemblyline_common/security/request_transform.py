"""
Request/Response Transform Policy

Kong Request Transformer and MuleSoft Header/Body Transform policy equivalent.
Modify requests and responses on-the-fly.

Features:
- Header manipulation (add, remove, rename, replace)
- Body transformation (JSON path, template)
- Query parameter manipulation
- Path rewriting
- Content-Type conversion
"""

import json
import logging
import re
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Callable, Dict, List, Optional, Pattern, Tuple, Union

from fastapi import FastAPI, Request, Response, HTTPException
from starlette.middleware.base import BaseHTTPMiddleware
from starlette.datastructures import Headers, MutableHeaders

logger = logging.getLogger(__name__)


class TransformAction(str, Enum):
    """Transform action types."""
    ADD = "add"  # Add if not exists
    SET = "set"  # Always set (overwrite)
    REMOVE = "remove"  # Remove if exists
    RENAME = "rename"  # Rename header/param
    REPLACE = "replace"  # Replace value using regex
    APPEND = "append"  # Append to existing value


@dataclass
class HeaderTransform:
    """Header transformation rule."""
    action: TransformAction
    name: str
    value: Optional[str] = None
    new_name: Optional[str] = None  # For rename
    pattern: Optional[str] = None  # For replace (regex)
    replacement: Optional[str] = None  # For replace


@dataclass
class QueryTransform:
    """Query parameter transformation rule."""
    action: TransformAction
    name: str
    value: Optional[str] = None
    new_name: Optional[str] = None
    pattern: Optional[str] = None
    replacement: Optional[str] = None


@dataclass
class BodyTransform:
    """Body transformation rule."""
    action: TransformAction
    path: str  # JSON path (e.g., "$.data.name")
    value: Optional[Any] = None
    template: Optional[str] = None  # Jinja2-like template


@dataclass
class PathTransform:
    """URL path transformation rule."""
    pattern: str  # Regex pattern to match
    replacement: str  # Replacement string (can use groups)
    stop_on_match: bool = True  # Stop after first match


@dataclass
class RequestTransformConfig:
    """Configuration for request transformation."""
    # Header transformations
    headers: List[HeaderTransform] = field(default_factory=list)

    # Query parameter transformations
    query_params: List[QueryTransform] = field(default_factory=list)

    # Body transformations
    body: List[BodyTransform] = field(default_factory=list)

    # Path rewrites
    paths: List[PathTransform] = field(default_factory=list)

    # Apply to specific content types only
    content_types: List[str] = field(default_factory=lambda: ["application/json"])

    # Remove empty values after transform
    remove_empty: bool = False


@dataclass
class ResponseTransformConfig:
    """Configuration for response transformation."""
    # Header transformations
    headers: List[HeaderTransform] = field(default_factory=list)

    # Body transformations
    body: List[BodyTransform] = field(default_factory=list)

    # Apply to specific status codes only (empty = all)
    status_codes: List[int] = field(default_factory=list)

    # Apply to specific content types only
    content_types: List[str] = field(default_factory=lambda: ["application/json"])


class JSONPathHelper:
    """Simple JSON path helper for body transformations."""

    @staticmethod
    def get(data: Dict, path: str) -> Any:
        """
        Get value at JSON path.

        Args:
            data: Dictionary to query
            path: JSON path (e.g., "$.data.items[0].name")

        Returns:
            Value at path or None
        """
        if not path.startswith("$."):
            path = "$." + path

        parts = path[2:].split(".")
        current = data

        for part in parts:
            if not part:
                continue

            # Handle array index
            match = re.match(r"(\w+)\[(\d+)\]", part)
            if match:
                key, index = match.groups()
                if key and isinstance(current, dict):
                    current = current.get(key, {})
                if isinstance(current, list) and int(index) < len(current):
                    current = current[int(index)]
                else:
                    return None
            elif isinstance(current, dict):
                current = current.get(part)
                if current is None:
                    return None
            else:
                return None

        return current

    @staticmethod
    def set(data: Dict, path: str, value: Any) -> Dict:
        """
        Set value at JSON path.

        Args:
            data: Dictionary to modify
            path: JSON path
            value: Value to set

        Returns:
            Modified dictionary
        """
        if not path.startswith("$."):
            path = "$." + path

        parts = path[2:].split(".")
        current = data

        for i, part in enumerate(parts[:-1]):
            if not part:
                continue

            match = re.match(r"(\w+)\[(\d+)\]", part)
            if match:
                key, index = match.groups()
                if key:
                    if key not in current:
                        current[key] = []
                    current = current[key]
                if isinstance(current, list):
                    idx = int(index)
                    while len(current) <= idx:
                        current.append({})
                    current = current[idx]
            else:
                if part not in current:
                    current[part] = {}
                current = current[part]

        # Set final value
        last_part = parts[-1]
        if last_part:
            match = re.match(r"(\w+)\[(\d+)\]", last_part)
            if match:
                key, index = match.groups()
                if key:
                    if key not in current:
                        current[key] = []
                    target = current[key]
                    idx = int(index)
                    while len(target) <= idx:
                        target.append(None)
                    target[idx] = value
            else:
                current[last_part] = value

        return data

    @staticmethod
    def remove(data: Dict, path: str) -> Dict:
        """
        Remove value at JSON path.

        Args:
            data: Dictionary to modify
            path: JSON path

        Returns:
            Modified dictionary
        """
        if not path.startswith("$."):
            path = "$." + path

        parts = path[2:].split(".")
        current = data

        for part in parts[:-1]:
            if not part:
                continue
            if isinstance(current, dict) and part in current:
                current = current[part]
            else:
                return data  # Path doesn't exist

        last_part = parts[-1]
        if last_part and isinstance(current, dict) and last_part in current:
            del current[last_part]

        return data


class RequestTransformer:
    """
    Request transformation policy.

    Usage:
        transformer = RequestTransformer(RequestTransformConfig(
            headers=[
                HeaderTransform(action=TransformAction.ADD, name="X-Custom", value="value"),
                HeaderTransform(action=TransformAction.REMOVE, name="X-Debug"),
            ],
            body=[
                BodyTransform(action=TransformAction.SET, path="$.meta.timestamp", value="${timestamp}"),
            ]
        ))

        # Transform request
        modified_request = await transformer.transform_request(request)
    """

    def __init__(self, config: Optional[RequestTransformConfig] = None):
        self.config = config or RequestTransformConfig()
        self._path_patterns: List[Tuple[Pattern, str, bool]] = []
        self._compile_patterns()

    def _compile_patterns(self):
        """Compile path rewrite patterns."""
        self._path_patterns = []
        for path_transform in self.config.paths:
            pattern = re.compile(path_transform.pattern)
            self._path_patterns.append(
                (pattern, path_transform.replacement, path_transform.stop_on_match)
            )

    def _apply_header_transforms(
        self,
        headers: MutableHeaders,
        transforms: List[HeaderTransform]
    ) -> MutableHeaders:
        """Apply header transformations."""
        for transform in transforms:
            name = transform.name.lower()

            if transform.action == TransformAction.ADD:
                if name not in headers:
                    headers[name] = transform.value or ""

            elif transform.action == TransformAction.SET:
                headers[name] = transform.value or ""

            elif transform.action == TransformAction.REMOVE:
                if name in headers:
                    del headers[name]

            elif transform.action == TransformAction.RENAME:
                if name in headers and transform.new_name:
                    value = headers[name]
                    del headers[name]
                    headers[transform.new_name.lower()] = value

            elif transform.action == TransformAction.REPLACE:
                if name in headers and transform.pattern and transform.replacement:
                    value = headers[name]
                    headers[name] = re.sub(
                        transform.pattern,
                        transform.replacement,
                        value
                    )

            elif transform.action == TransformAction.APPEND:
                if name in headers:
                    headers[name] = headers[name] + (transform.value or "")
                else:
                    headers[name] = transform.value or ""

        return headers

    def _apply_body_transforms(
        self,
        body: Dict,
        transforms: List[BodyTransform]
    ) -> Dict:
        """Apply body transformations."""
        for transform in transforms:
            if transform.action == TransformAction.ADD:
                existing = JSONPathHelper.get(body, transform.path)
                if existing is None:
                    body = JSONPathHelper.set(body, transform.path, transform.value)

            elif transform.action == TransformAction.SET:
                body = JSONPathHelper.set(body, transform.path, transform.value)

            elif transform.action == TransformAction.REMOVE:
                body = JSONPathHelper.remove(body, transform.path)

            elif transform.action == TransformAction.REPLACE:
                existing = JSONPathHelper.get(body, transform.path)
                if existing is not None and transform.pattern and transform.replacement:
                    if isinstance(existing, str):
                        new_value = re.sub(transform.pattern, transform.replacement, existing)
                        body = JSONPathHelper.set(body, transform.path, new_value)

        return body

    def transform_path(self, path: str) -> str:
        """Transform URL path using rewrite rules."""
        for pattern, replacement, stop_on_match in self._path_patterns:
            new_path, count = pattern.subn(replacement, path)
            if count > 0:
                logger.debug(f"Path rewrite: {path} -> {new_path}")
                path = new_path
                if stop_on_match:
                    break
        return path

    async def transform_request(
        self,
        request: Request,
        body: Optional[bytes] = None
    ) -> Tuple[Dict[str, str], Optional[bytes], str]:
        """
        Transform request.

        Args:
            request: FastAPI request
            body: Optional body bytes (if already read)

        Returns:
            Tuple of (modified_headers, modified_body, modified_path)
        """
        # Copy headers
        headers_dict = dict(request.headers)
        mutable_headers = MutableHeaders(scope={"type": "http", "headers": []})
        for k, v in headers_dict.items():
            mutable_headers[k] = v

        # Apply header transforms
        mutable_headers = self._apply_header_transforms(
            mutable_headers,
            self.config.headers
        )

        # Transform path
        modified_path = self.transform_path(request.url.path)

        # Transform body if JSON
        modified_body = body
        content_type = request.headers.get("content-type", "")

        if any(ct in content_type for ct in self.config.content_types):
            if body is None:
                body = await request.body()

            if body and self.config.body:
                try:
                    body_dict = json.loads(body)
                    body_dict = self._apply_body_transforms(body_dict, self.config.body)
                    modified_body = json.dumps(body_dict).encode()
                except json.JSONDecodeError:
                    logger.warning("Failed to parse request body as JSON")

        return dict(mutable_headers), modified_body, modified_path


class ResponseTransformer:
    """
    Response transformation policy.

    Usage:
        transformer = ResponseTransformer(ResponseTransformConfig(
            headers=[
                HeaderTransform(action=TransformAction.REMOVE, name="X-Powered-By"),
                HeaderTransform(action=TransformAction.ADD, name="X-Custom", value="value"),
            ]
        ))

        # Transform response
        modified_response = await transformer.transform_response(response)
    """

    def __init__(self, config: Optional[ResponseTransformConfig] = None):
        self.config = config or ResponseTransformConfig()

    def _apply_header_transforms(
        self,
        headers: MutableHeaders,
        transforms: List[HeaderTransform]
    ) -> MutableHeaders:
        """Apply header transformations (same as request)."""
        for transform in transforms:
            name = transform.name.lower()

            if transform.action == TransformAction.ADD:
                if name not in headers:
                    headers[name] = transform.value or ""

            elif transform.action == TransformAction.SET:
                headers[name] = transform.value or ""

            elif transform.action == TransformAction.REMOVE:
                if name in headers:
                    del headers[name]

            elif transform.action == TransformAction.RENAME:
                if name in headers and transform.new_name:
                    value = headers[name]
                    del headers[name]
                    headers[transform.new_name.lower()] = value

            elif transform.action == TransformAction.REPLACE:
                if name in headers and transform.pattern and transform.replacement:
                    value = headers[name]
                    headers[name] = re.sub(transform.pattern, transform.replacement, value)

            elif transform.action == TransformAction.APPEND:
                if name in headers:
                    headers[name] = headers[name] + (transform.value or "")
                else:
                    headers[name] = transform.value or ""

        return headers

    def _apply_body_transforms(
        self,
        body: Dict,
        transforms: List[BodyTransform]
    ) -> Dict:
        """Apply body transformations."""
        for transform in transforms:
            if transform.action == TransformAction.ADD:
                existing = JSONPathHelper.get(body, transform.path)
                if existing is None:
                    body = JSONPathHelper.set(body, transform.path, transform.value)

            elif transform.action == TransformAction.SET:
                body = JSONPathHelper.set(body, transform.path, transform.value)

            elif transform.action == TransformAction.REMOVE:
                body = JSONPathHelper.remove(body, transform.path)

        return body

    def should_transform(self, status_code: int, content_type: str) -> bool:
        """Check if response should be transformed."""
        # Check status code
        if self.config.status_codes and status_code not in self.config.status_codes:
            return False

        # Check content type
        if self.config.content_types:
            if not any(ct in content_type for ct in self.config.content_types):
                return False

        return True

    async def transform_response(
        self,
        response: Response,
        body: Optional[bytes] = None
    ) -> Tuple[Dict[str, str], Optional[bytes]]:
        """
        Transform response.

        Args:
            response: FastAPI response
            body: Optional body bytes

        Returns:
            Tuple of (modified_headers, modified_body)
        """
        # Check if we should transform
        content_type = response.headers.get("content-type", "")
        if not self.should_transform(response.status_code, content_type):
            return dict(response.headers), body

        # Apply header transforms
        mutable_headers = MutableHeaders(scope={"type": "http", "headers": []})
        for k, v in response.headers.items():
            mutable_headers[k] = v

        mutable_headers = self._apply_header_transforms(
            mutable_headers,
            self.config.headers
        )

        # Transform body if JSON
        modified_body = body
        if body and self.config.body and "application/json" in content_type:
            try:
                body_dict = json.loads(body)
                body_dict = self._apply_body_transforms(body_dict, self.config.body)
                modified_body = json.dumps(body_dict).encode()
            except json.JSONDecodeError:
                logger.warning("Failed to parse response body as JSON")

        return dict(mutable_headers), modified_body


class TransformMiddleware(BaseHTTPMiddleware):
    """
    Combined request/response transformation middleware.

    Usage:
        app.add_middleware(
            TransformMiddleware,
            request_config=RequestTransformConfig(...),
            response_config=ResponseTransformConfig(...),
        )
    """

    def __init__(
        self,
        app,
        request_config: Optional[RequestTransformConfig] = None,
        response_config: Optional[ResponseTransformConfig] = None,
    ):
        super().__init__(app)
        self.request_transformer = RequestTransformer(request_config) if request_config else None
        self.response_transformer = ResponseTransformer(response_config) if response_config else None

    async def dispatch(self, request: Request, call_next):
        # Note: Full request transformation requires custom ASGI handling
        # This middleware handles response transformation

        response = await call_next(request)

        if self.response_transformer:
            # Read response body
            body = b""
            async for chunk in response.body_iterator:
                body += chunk

            # Transform
            headers, modified_body = await self.response_transformer.transform_response(
                response, body
            )

            # Create new response
            from starlette.responses import Response as StarletteResponse
            return StarletteResponse(
                content=modified_body or body,
                status_code=response.status_code,
                headers=headers,
                media_type=response.media_type,
            )

        return response


# Preset configurations
TRANSFORM_PRESETS = {
    "security_headers": ResponseTransformConfig(
        headers=[
            HeaderTransform(action=TransformAction.REMOVE, name="X-Powered-By"),
            HeaderTransform(action=TransformAction.REMOVE, name="Server"),
            HeaderTransform(action=TransformAction.SET, name="X-Content-Type-Options", value="nosniff"),
            HeaderTransform(action=TransformAction.SET, name="X-Frame-Options", value="DENY"),
            HeaderTransform(action=TransformAction.SET, name="X-XSS-Protection", value="1; mode=block"),
            HeaderTransform(action=TransformAction.SET, name="Strict-Transport-Security", value="max-age=31536000; includeSubDomains"),
        ]
    ),
    "api_version": RequestTransformConfig(
        headers=[
            HeaderTransform(action=TransformAction.ADD, name="X-API-Version", value="v1"),
        ]
    ),
    "request_id": RequestTransformConfig(
        headers=[
            HeaderTransform(action=TransformAction.ADD, name="X-Request-ID", value="${uuid}"),
        ]
    ),
}


def get_request_transformer(config: Optional[RequestTransformConfig] = None) -> RequestTransformer:
    """Create request transformer."""
    return RequestTransformer(config)


def get_response_transformer(config: Optional[ResponseTransformConfig] = None) -> ResponseTransformer:
    """Create response transformer."""
    return ResponseTransformer(config)
