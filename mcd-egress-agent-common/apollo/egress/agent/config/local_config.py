import os
from typing import Optional, Dict

from apollo.egress.agent.config.config_persistence import ConfigurationPersistence

_SNA_ENV_PREFIX = "SNA"


class LocalConfig(ConfigurationPersistence):
    def get_value(self, key: str) -> Optional[str]:
        return os.getenv(f"{_SNA_ENV_PREFIX}_{key}")

    def set_value(self, key: str, value: str):
        raise NotImplementedError(
            "You cannot update config settings in a local environment, update env vars instead"
        )

    def get_all_values(self) -> Dict[str, str]:
        return {
            key: value
            for key, value in os.environ.items()
            if key.startswith(_SNA_ENV_PREFIX)
        }
