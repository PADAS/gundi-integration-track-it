import httpx
import backoff
import logging

from app.actions.configurations import get_auth_config
from app.services.state import IntegrationStateManager


state_manager = IntegrationStateManager()
logger = logging.getLogger(__name__)


@backoff.on_predicate(backoff.constant, jitter=None, interval=60)
async def get_positions_list(integration):
    url = integration.base_url
    auth = get_auth_config(integration)
    params = {
        "token": "getLiveData",
        "user": auth.username,
        "pass": auth.password,
        "format": "json"
    }

    async with httpx.AsyncClient(timeout=120) as session:
        response = await session.get(
            url,
            params=params
        )
        response.raise_for_status()
        response = response.json()

    result = response['root']
    if "error" in result:
        logger.info("Waiting 1 min to make the request...")
        return False
    return response['root'].get("VehicleData")
