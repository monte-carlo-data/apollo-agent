from logging import Logger, LoggerAdapter
from typing import Any, Dict, cast


class AnnotatedLoggerAdapter(LoggerAdapter):
    def __init__(self, logger: Logger, extra: Dict):
        super().__init__(logger, extra)

    def process(self, msg: str, kwargs: Any):
        if type(self.extra) is type(dict()) and isinstance(kwargs.get("extra"), dict):
            extra = self.extra or {}
            kwargs["extra"] = {**extra, **kwargs["extra"]}
        else:
            kwargs["extra"] = self.extra
        return msg, kwargs


def annotate_logger(logger: Logger, extra: Dict) -> Logger:
    return cast(Logger, AnnotatedLoggerAdapter(logger, extra))
