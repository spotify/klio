"""Variables and functions shared between the stages of integration testing."""
import os
import yaml

from klio_core import config

def get_config():
    """Load KlioConfig object."""
    config_path = os.path.join(os.path.dirname(__file__), "..", "klio-job.yaml")
    try:
        with open(config_path) as f:
            cfg_dict = yaml.safe_load(f)

        return config.KlioConfig(cfg_dict)

    except IOError as e:
        logging.error(e)
        raise SystemExit(1)

entity_ids = ['1', '2', '3', '4', '5']

