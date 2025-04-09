import os
import pathlib

from app.app import app
from app.utils.log import Log


def main():
    os.environ["APP_INSTALLDIR"] = os.path.dirname(os.path.abspath(__file__))
    BASEDIR = pathlib.Path().resolve()
    os.environ["APP_BASEDIR"] = str(BASEDIR)
    Log.configure_logging(BASEDIR)
    app()


if __name__ == "__main__":
    main()
