import asyncio
import hashlib
import json
import logging
import sys

import yaml
from aiomqtt import Client, MqttError

from dess_monitor.api.client import (
    auth_user,
    get_devices,
    get_device_last_data,
    get_device_energy_flow,
    get_device_ctrl_fields,
    set_ctrl_device_param,
    AuthInvalidateError,
)

# Configure root logger
logging.basicConfig(
    level=logging.DEBUG,
    format="%(asctime)s %(levelname)-8s %(name)s: %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)

# Windows event loop policy
if sys.platform.startswith("win"):
    from asyncio import WindowsSelectorEventLoopPolicy

    asyncio.set_event_loop_policy(WindowsSelectorEventLoopPolicy())


def load_config(path: str = "config.yaml") -> dict:
    logger.debug("Loading configuration from %s", path)
    with open(path) as f:
        cfg = yaml.safe_load(f)
    logger.info(
        "Configuration loaded: MQTT %s:%d, devices=%s",
        cfg['mqtt']['host'],
        cfg['mqtt']['port'],
        [d['pn'] for d in cfg.get('devices', [])]
    )
    return cfg


async def refresh_auth(cfg: dict, state: dict):
    """
    Refresh authentication token/secret and store in state.
    """
    logger.info("Refreshing authentication")
    # choose or compute password_hash
    if 'password_hash' in state:
        pwd_hash = state['password_hash']
        logger.debug("Using cached password_hash")
    elif cfg.get('password_hash'):
        pwd_hash = cfg['password_hash']
        logger.debug("Using config.password_hash")
    elif cfg.get('password'):
        pwd_hash = hashlib.sha1(cfg['password'].encode()).hexdigest()
        logger.debug("Computed SHA1 of config.password")
    else:
        logger.error("No password or password_hash in config")
        raise RuntimeError("Authentication credentials missing")
    state['password_hash'] = pwd_hash

    try:
        auth = await auth_user(cfg['username'], pwd_hash)
        state.update({
            'token': auth['token'],
            'secret': auth['secret'],
            'expire': auth.get('expire'),
            'issued_at': asyncio.get_event_loop().time(),
        })
        logger.info("Authenticated; token valid for %s seconds", state['expire'])
    except Exception as e:
        logger.exception("Failed to authenticate: %s", e)
        raise


async def publish_data(cfg: dict, mqtt_client: Client, state: dict, devices: list):
    """
    Loop to fetch and publish data for each device at regular intervals.
    """
    interval = cfg.get("poll_interval", 10)
    logger.info("Starting publish loop (interval=%ds)", interval)
    while True:
        for dev in devices:
            pn = dev['pn']
            base = f"{cfg['mqtt']['base_topic']}/{pn}"
            logger.debug(">>> Begin publish for PN=%s", pn)

            # Publish static metadata
            for k, v in dev.items():
                if isinstance(v, (str, int, float, bool)):
                    topic = f"{base}/device/{k}"
                    logger.debug("Publishing metadata %s = %s", topic, v)
                    await mqtt_client.publish(topic, str(v), qos=1, retain=True)

            # last_data
            try:
                logger.debug("Fetching last_data for PN=%s", pn)
                last = await get_device_last_data(state['token'], state['secret'], dev)
            except AuthInvalidateError:
                logger.warning("last_data: auth invalid for PN=%s", pn)
                raise
            except Exception as e:
                logger.error("Error fetching last_data for PN=%s: %s", pn, e)
                last = {}

            for entries in last.get("pars", {}).values():
                for e in entries:
                    t_val = f"{base}/last_data/{e['id']}"
                    logger.debug("Publishing last_data value %s = %s", t_val, e.get('val'))
                    await mqtt_client.publish(t_val, str(e.get('val', '')), qos=1, retain=True)
                    if 'unit' in e:
                        t_unit = f"{t_val}/unit"
                        logger.debug("Publishing last_data unit %s = %s", t_unit, e['unit'])
                        await mqtt_client.publish(t_unit, e['unit'], qos=1, retain=True)

            # energy_flow
            try:
                logger.debug("Fetching energy_flow for PN=%s", pn)
                flow = await get_device_energy_flow(state['token'], state['secret'], dev)
            except AuthInvalidateError:
                logger.warning("energy_flow: auth invalid for PN=%s", pn)
                raise
            except Exception as e:
                logger.error("Error fetching energy_flow for PN=%s: %s", pn, e)
                flow = {}

            for entries in flow.values():
                if isinstance(entries, list):
                    for e in entries:
                        key = e.get("par", "").replace(" ", "_")
                        t_val = f"{base}/energy_flow/{key}"
                        logger.debug("Publishing energy_flow %s = %s", t_val, e.get('val'))
                        await mqtt_client.publish(t_val, str(e.get('val', '')), qos=1, retain=True)
                        if 'unit' in e:
                            t_unit = f"{t_val}/unit"
                            logger.debug("Publishing energy_flow unit %s = %s", t_unit, e['unit'])
                            await mqtt_client.publish(t_unit, e['unit'], qos=1, retain=True)

            # control fields
            try:
                logger.debug("Fetching ctrl_fields for PN=%s", pn)
                ctrl = await get_device_ctrl_fields(state['token'], state['secret'], dev)
            except AuthInvalidateError:
                logger.warning("ctrl_fields: auth invalid for PN=%s", pn)
                raise
            except Exception as e:
                logger.error("Error fetching ctrl_fields for PN=%s: %s", pn, e)
                ctrl = {'field': []}

            for f in ctrl.get("field", []):
                fid = f['id']
                base_ctrl = f"{base}/ctrl_fields/{fid}"
                logger.debug("Publishing ctrl field name %s/name = %s", base_ctrl, f.get('name', fid))
                await mqtt_client.publish(f"{base_ctrl}/name", f.get("name", fid), qos=1, retain=True)

                if 'item' in f:
                    opts = f['item']
                    logger.debug("Publishing options %s/options = %s", base_ctrl, opts)
                    await mqtt_client.publish(f"{base_ctrl}/options", json.dumps(opts, ensure_ascii=False), qos=1,
                                              retain=True)
                elif 'unit' in f:
                    logger.debug("Publishing unit %s/unit = %s", base_ctrl, f['unit'])
                    await mqtt_client.publish(f"{base_ctrl}/unit", f['unit'], qos=1, retain=True)
                elif 'hint' in f:
                    logger.debug("Publishing hint %s/hint = %s", base_ctrl, f['hint'])
                    await mqtt_client.publish(f"{base_ctrl}/hint", f['hint'], qos=1, retain=True)

            logger.debug("<<< Finished publish for PN=%s", pn)

        logger.debug("Sleeping for %ds before next publish cycle", interval)
        await asyncio.sleep(interval)


async def command_listener(cfg: dict, mqtt_client: Client, state: dict, devices: list):
    """
    Listen for per-field set commands on
    `<base>/ctrl_fields/<field_id>/set` topics.
    """
    async for msg in mqtt_client.messages:
        topic = msg.topic.value if hasattr(msg.topic, "value") else str(msg.topic)
        parts = topic.split("/")
        logger.debug("Received MQTT message on %s", topic)

        if len(parts) < 5 or parts[-1] != "set":
            continue

        pn = parts[-4]
        fid = parts[-2]
        val = msg.payload.decode().strip()
        status_topic = f"{cfg['mqtt']['base_topic']}/{pn}/ctrl_fields/{fid}/set_status"

        dev = next((d for d in devices if d['pn'] == pn), None)
        if not dev:
            logger.warning("Command for unknown PN=%s", pn)
            await mqtt_client.publish(status_topic, "error: unknown PN", qos=1, retain=True)
            continue

        logger.info("Applying cmd PN=%s field=%s value=%s", pn, fid, val)
        try:
            await set_ctrl_device_param(state['token'], state['secret'], dev, fid, val)
            logger.info("Command succeeded PN=%s field=%s", pn, fid)
            await mqtt_client.publish(status_topic, "ok", qos=1, retain=True)
        except AuthInvalidateError:
            logger.warning("CommandListener: auth invalid while setting PN=%s", pn)
            raise
        except Exception as e:
            logger.error("Command failed PN=%s field=%s: %s", pn, fid, e)
            await mqtt_client.publish(status_topic, f"error: {e}", qos=1, retain=True)


async def run(cfg: dict):
    state: dict = {}
    # initial auth
    await refresh_auth(cfg, state)

    # load devices list
    try:
        api_devs = await get_devices(state['token'], state['secret'])
        logger.info("Retrieved %d devices from API", len(api_devs))
    except Exception as e:
        logger.exception("Failed to fetch devices list: %s", e)
        return

    devices = [
        d for d in api_devs
        if d.get('status') != 1 and any(d['pn'] == c['pn'] for c in cfg.get('devices', []))
    ]
    logger.info("Filtered to %d active devices", len(devices))
    if not devices:
        logger.error("No matching devices, exiting")
        return

    # connect to MQTT
    async with Client(
            hostname=cfg['mqtt']['host'],
            port=cfg['mqtt']['port'],
            username=cfg['mqtt'].get('user'),
            password=cfg['mqtt'].get('pass'),
            timeout=30,
            keepalive=60,
    ) as mqtt_client:
        logger.info("Connected to MQTT broker %s:%d", cfg['mqtt']['host'], cfg['mqtt']['port'])
        sub_topic = f"{cfg['mqtt']['base_topic']}/+/ctrl_fields/+/set"
        await mqtt_client.subscribe(sub_topic)
        logger.info("Subscribed to control-set topic pattern %s", sub_topic)

        # super-loop: restart on AuthInvalidateError
        while True:
            pub_task = asyncio.create_task(publish_data(cfg, mqtt_client, state, devices))
            cmd_task = asyncio.create_task(command_listener(cfg, mqtt_client, state, devices))
            try:
                await asyncio.gather(pub_task, cmd_task)
                break  # tasks ended normally
            except AuthInvalidateError:
                logger.warning("Auth expired/invalid, refreshing and restarting loops")
                pub_task.cancel()
                cmd_task.cancel()
                await refresh_auth(cfg, state)
            except Exception:
                logger.exception("Unexpected error in run loop, aborting")
                raise


if __name__ == "__main__":
    cfg = load_config()
    try:
        asyncio.run(run(cfg))
    except MqttError as e:
        logger.error("MQTT error: %s", e)
    except Exception as e:
        logger.exception("Fatal error: %s", e)
