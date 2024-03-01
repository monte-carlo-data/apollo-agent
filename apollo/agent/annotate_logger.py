from logging import Logger, LoggerAdapter
from typing import Any, Dict, cast


class AnnotatedLoggerAdapter(LoggerAdapter):
    """Use this class to add extra fields to the log record on every log call."""

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
    """
    Returns a new logger that adds extra fields to the log record on every log call.
    :param logger: The logger to annotate.
    :param extra: The extra fields to add to the log record.
    """
    return cast(Logger, AnnotatedLoggerAdapter(logger, extra))
