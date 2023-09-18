import logging
import logging.handlers
from typing import Optional

from sintetizador.utils.singleton import Singleton


class Log(metaclass=Singleton):
    LOGGER = None

    @classmethod
    def configure_logging(cls, diretorio: str):
        root = logging.getLogger("main")
        f = logging.Formatter("%(asctime)s %(levelname)s: %(message)s")
        # Logger para STDOUT
        std_h = logging.StreamHandler()
        std_h.setFormatter(f)
        root.addHandler(std_h)
        root.setLevel(logging.INFO)
        cls.LOGGER = root

    @classmethod
    def log(cls) -> Optional[logging.Logger]:
        return cls.LOGGER
