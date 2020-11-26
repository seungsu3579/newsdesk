import yaml
import os 
from datetime import datetime
from pathlib import Path

# environment = os.environ.get('ENVIRONMENT', 'secret_key')
with Path(f"config/secret_key.yaml").open() as config_file:
    CONFIG = yaml.load(config_file, Loader=yaml.FullLoader)
    CONFIG['excution_date'] = datetime.today().strftime("%Y-%m-%d")