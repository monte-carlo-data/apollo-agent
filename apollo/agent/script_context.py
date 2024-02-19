import logging


class ScriptContext:
    def __init__(self, logger: logging.Logger):
        self._logger = logger

    @property
    def logger(self):
        return self._logger
