from logging import Formatter, LogRecord
from typing import Optional

from apollo.agent.redact import AgentRedactUtilities


class RedactFormatterWrapper(Formatter):
    def __init__(self, formatter: Optional[Formatter]):
        super().__init__()
        self._formatter = formatter or Formatter()

    def format(self, record: LogRecord):
        return self._formatter.format(self._redact_record(record))

    @staticmethod
    def _redact_record(record: LogRecord):
        record.msg = AgentRedactUtilities.standard_redact(record.msg)
        return record
