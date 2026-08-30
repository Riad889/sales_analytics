from pathlib import Path
from dynaconf import Dynaconf

BASE_DIR = Path(__file__).resolve().parent
CONFIG_FILE = BASE_DIR / "config.toml"

config = Dynaconf(settings_files=[str(CONFIG_FILE)])
