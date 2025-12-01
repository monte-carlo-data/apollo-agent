from typing import Dict, Optional

from apollo.egress.agent.config.config_persistence import ConfigurationPersistence


class ConfigurationManager:
    def __init__(self, persistence: ConfigurationPersistence):
        self._persistence = persistence

    def get_optional_str_value(self, key: str) -> Optional[str]:
        return self._get_value(key)

    def get_str_value(self, key: str, default_value: str) -> str:
        return self._get_value(key) or default_value

    def get_int_value(self, key: str, default_value: int) -> int:
        if value := self._get_value(key):
            return int(value)
        else:
            return default_value

    def get_bool_value(self, key: str, default_value: bool) -> bool:
        if value := self._get_value(key):
            return value.lower() == "true"
        else:
            return default_value

    def get_all_values(self):
        return self._persistence.get_all_values()

    def _get_value(self, key: str) -> Optional[str]:
        return self._persistence.get_value(key)

    def set_values(self, values: Dict[str, str]):
        for key, value in values.items():
            self._persistence.set_value(key, value)
