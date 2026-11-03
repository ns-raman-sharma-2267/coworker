import tomllib
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]  
     
with open(ROOT / "config.toml", "rb") as f:     
    CFG = tomllib.load(f)

DATA_DIR = ROOT / CFG["paths"]["data_dir"]
DATA_DIR.mkdir(exist_ok=True)
PROJECTS_ROOT = Path(CFG["paths"]["projects_root"])