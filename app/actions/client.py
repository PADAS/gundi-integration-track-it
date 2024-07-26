import httpx
import backoff
import logging

from app.actions.configurations import (
    AuthenticateConfig,
    FetchSamplesConfig,
    PullObservationsConfig
)
from app.services.errors import ConfigurationNotFound
from app.services.utils import find_config_for_action
from app.services.state import IntegrationStateManager


state_manager = IntegrationStateManager()
logger = logging.getLogger(__name__)


def get_auth_config(integration):
    # Look for the login credentials, needed for any action
    auth_config = find_config_for_action(
        configurations=integration.configurations,
        action_id="auth"
    )
    if not auth_config:
        raise ConfigurationNotFound(
            f"Authentication settings for integration {str(integration.id)} "
            f"are missing. Please fix the integration setup in the portal."
        )
    return AuthenticateConfig.parse_obj(auth_config.data)


def get_fetch_samples_config(integration):
    # Look for the login credentials, needed for any action
    fetch_samples_config = find_config_for_action(
        configurations=integration.configurations,
        action_id="fetch_samples"
    )
    if not fetch_samples_config:
        raise ConfigurationNotFound(
            f"fetch_samples settings for integration {str(integration.id)} "
            f"are missing. Please fix the integration setup in the portal."
        )
    return FetchSamplesConfig.parse_obj(fetch_samples_config.data)


def get_pull_config(integration):
    # Look for the login credentials, needed for any action
    pull_config = find_config_for_action(
        configurations=integration.configurations,
        action_id="pull_observations"
    )
    if not pull_config:
        raise ConfigurationNotFound(
            f"pull_config settings for integration {str(integration.id)} "
            f"are missing. Please fix the integration setup in the portal."
        )
    return PullObservationsConfig.parse_obj(pull_config.data)


@backoff.on_predicate(backoff.constant, jitter=None, interval=60)
async def get_positions_list(integration, config):
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
