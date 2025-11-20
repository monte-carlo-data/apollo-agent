import json
import time
from datetime import datetime
from logging import Formatter, LogRecord, Logger
from typing import Any, Optional, Dict

from apollo.agent.redact import AgentRedactUtilities


class JsonLogFormatter(Formatter):
    """
    Logging formatter that formats log messages as a JSON document, supports logging "extra" as an additional
    attribute.
    """

    def format(self, record: LogRecord) -> str:
        msg = super().format(record)
        try:
            message = {
                "ts": datetime(*time.gmtime(record.created)[:7]).isoformat(),
                "level": record.levelname,
                "msg": msg,
                "module": record.module,
                "function_name": record.funcName,
                "line_number": record.lineno,
            }
            if hasattr(record, "extra"):
                message["extra"] = record.extra  # type: ignore

            if record.exc_info and record.exc_info[0]:
                message["exception"] = {
                    "type": record.exc_info[0].__name__,
                    "message": str(record.exc_info[1]),
                    "stack": self.formatException(record.exc_info),
                }
            result = json.dumps(AgentRedactUtilities.standard_redact(message))
        except Exception as e:
            result = json.dumps(
                AgentRedactUtilities.standard_redact(
                    {"level": "error", "msg": msg, "invalid_log_error": str(e)}
                )
            )

        # add newline to force log flush
        return f"{result}\n"


class ExtraLogger(Logger):
    """
    Logger subclass that sets the extra dictionary in record.extra so it can be logged as a separate attribute
    """

    def makeRecord(
        self,
        name: str,
        level: int,
        fn: str,
        lno: int,
        msg: object,
        args: Any,
        exc_info: Any,
        func: Optional[str] = None,
        extra: Optional[Dict] = None,
        sinfo: Optional[str] = None,
    ) -> LogRecord:
        record = super().makeRecord(
            name, level, fn, lno, msg, args, exc_info, func, None, sinfo
        )
        if extra:
            record.extra = extra
        return record
