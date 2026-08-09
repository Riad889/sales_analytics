from pathlib import Path
from dynaconf import Dynaconf

BASE_DIR = Path(__file__).resolve().parent
breakpoint()

config = Dynaconf(
    settings_files=[f"{BASE_DIR}/config/settings.toml"],
)
