import os
from pathlib import Path

import yaml

PATH_PROJECT = Path(__file__).parent.parent

params = yaml.safe_load(open(PATH_PROJECT / "params.yaml"))