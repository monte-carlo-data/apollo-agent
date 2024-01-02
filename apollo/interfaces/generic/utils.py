from datetime import datetime, timezone
from typing import Optional


class AgentPlatformUtils:
    @staticmethod
    def parse_datetime(
        dt_str: Optional[str], default_value: Optional[datetime] = None
    ) -> Optional[datetime]:
        if not dt_str:
            return default_value
        dt = datetime.fromisoformat(dt_str)
        if not dt.tzinfo:
            dt = dt.astimezone(timezone.utc)  # make it offset-aware
        return dt
