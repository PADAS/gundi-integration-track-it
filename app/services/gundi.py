import json
import datetime
import logging
from urllib.parse import urlparse
import httpx
import stamina
from gundi_client_v2.client import GundiClient, GundiDataSenderClient


@stamina.retry(on=httpx.HTTPError, attempts=3, wait_initial=datetime.timedelta(seconds=1), wait_max=datetime.timedelta(seconds=10))
async def get_gundi_api_key(integration_id):
    async with GundiClient() as gundi_client:
        return await gundi_client.get_integration_api_key(
            integration_id=integration_id
        )


async def _get_sensors_api_client(integration_id):
    gundi_api_key = await get_gundi_api_key(integration_id=integration_id)
    assert gundi_api_key, f"Cannot get a valid API Key for integration {integration_id}"
    sensors_api_client = GundiDataSenderClient(
        integration_api_key=gundi_api_key
    )
    return sensors_api_client


@stamina.retry(on=httpx.HTTPError, attempts=3, wait_initial=datetime.timedelta(seconds=1), wait_max=datetime.timedelta(seconds=10))
async def send_events_to_gundi(events, **kwargs):
    # Push events to Gundi using the REST API v2
    integration_id = kwargs.get("integration_id")
    assert integration_id, "integration_id is required"
    sensors_api_client = await _get_sensors_api_client(integration_id=integration_id)
    return await sensors_api_client.post_events(data=events)


@stamina.retry(on=httpx.HTTPError, attempts=3, wait_initial=datetime.timedelta(seconds=1), wait_max=datetime.timedelta(seconds=10))
async def send_observations_to_gundi(observations, **kwargs):
    # Push observations to Gundi using the REST API v2
    integration_id = kwargs.get("integration_id")
    assert integration_id, "integration_id is required"
    sensors_api_client = await _get_sensors_api_client(integration_id=integration_id)
    return await sensors_api_client.post_observations(data=observations)
