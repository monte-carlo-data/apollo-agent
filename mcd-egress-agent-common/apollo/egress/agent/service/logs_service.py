import logging
from datetime import datetime, timezone
from typing import List, Dict, Any

from apollo.egress.agent.utils.utils import LOCAL

logger = logging.getLogger(__name__)


class LogsService:
    def get_logs(self, limit: int) -> List[Dict[str, Any]]:
        if LOCAL:
            # In local mode, we don't have access to the database, so we return a dummy record
            return [
                {
                    "timestamp": datetime.now(tz=timezone.utc).isoformat(),
                    "message": "This is a dummy log message.",
                }
            ]
        logger.warning("LogsService not implemented for non-local mode")
        return []
