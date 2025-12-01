import logging
from typing import List

from apollo.egress.agent.utils.utils import LOCAL

logger = logging.getLogger(__name__)


class MetricsService:
    @staticmethod
    def fetch_metrics() -> List[str]:
        if LOCAL:
            return LocalMetricsService.fetch_metrics()
        logger.warning("Metrics not implemented for non-local mode")
        return LocalMetricsService.fetch_metrics()


class LocalMetricsService:
    @staticmethod
    def fetch_metrics() -> List[str]:
        # used only for testing
        return ['metric_1{host="abc.com",resource="cpu"} 1', "metric_2 2"]
