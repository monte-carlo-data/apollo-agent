from datetime import datetime
from typing import List, Dict, Optional
from apollo.agent.updater import AgentUpdater


class AzureUpdater(AgentUpdater):
    def update(
        self, image: Optional[str], timeout_seconds: Optional[int], **kwargs  # type: ignore
    ) -> Dict:
        return {}

    def get_current_image(self) -> Optional[str]:
        return "no image"

    def get_update_logs(self, start_time: datetime, limit: int) -> List[Dict]:
        return []
