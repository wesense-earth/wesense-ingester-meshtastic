"""Helpers for the per-region MQTT settings in config/mqtt_regions.json."""

import os


def region_password(region: str, config: dict) -> str:
    """Return the MQTT password for one region.

    A region either carries a literal "password" (the public Meshtastic broker credentials are
    published and are fine to keep in the file) or names an environment variable with
    "password_env", for a broker whose password must not be committed. If "password_env" is set
    and that variable is missing or empty this raises, rather than connecting with an empty
    password and failing later with a less obvious error.
    """
    env_name = config.get("password_env")
    if env_name:
        value = os.environ.get(env_name)
        if not value:
            raise RuntimeError(
                f"Region {region} takes its MQTT password from the environment variable "
                f"{env_name}, which is not set"
            )
        return value
    return config.get("password", "")
