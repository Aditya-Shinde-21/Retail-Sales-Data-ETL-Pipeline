from pathlib import Path
import yaml

def load_config(relative_path="resources/dev/yaml_config.yaml"):
    project_root = Path(__file__).resolve().parents[3]
    config_path = project_root / relative_path

    if not config_path.exists():
        raise FileNotFoundError(f"Config file not found at {config_path}")

    with open(config_path, "r") as f:
        return yaml.safe_load(f)