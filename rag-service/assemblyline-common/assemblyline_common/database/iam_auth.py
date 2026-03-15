"""RDS IAM Authentication token provider.

Generates short-lived (15 min) auth tokens using the instance's IAM role.
Tokens are cached and refreshed 5 minutes before expiry.

Works identically on EC2 (instance role) and ECS (task role) with zero code changes.
"""

import time
import logging

import boto3
from assemblyline_common.config import settings

logger = logging.getLogger(__name__)

_cached_token: str | None = None
_token_expires_at: float = 0
_TOKEN_REFRESH_MARGIN = 300  # Refresh 5 min before 15-min expiry


def get_rds_iam_token() -> str:
    """Generate or return cached RDS IAM auth token."""
    global _cached_token, _token_expires_at

    now = time.time()
    if _cached_token and now < _token_expires_at:
        return _cached_token

    client = boto3.client("rds", region_name=settings.AWS_REGION)
    token = client.generate_db_auth_token(
        DBHostname=settings.RDS_IAM_HOST,
        Port=settings.RDS_IAM_PORT,
        DBUsername=settings.RDS_IAM_USER,
        Region=settings.AWS_REGION,
    )

    _cached_token = token
    _token_expires_at = now + 900 - _TOKEN_REFRESH_MARGIN  # ~10 min effective
    logger.info(
        "RDS IAM token generated for %s@%s",
        settings.RDS_IAM_USER,
        settings.RDS_IAM_HOST,
    )
    return token
