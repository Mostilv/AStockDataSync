from __future__ import annotations

from pathlib import Path
from typing import Any, Dict

from .utils.config_loader import load_config


def resolve_config_path(path: str | None) -> Path:
    if path:
        return Path(path).resolve()
    return Path(__file__).resolve().parents[1] / "config.yaml"


def load_runtime_config(path: str | None = None) -> Dict[str, Any]:
    config_path = resolve_config_path(path)
    config = load_config(str(config_path))
    if not isinstance(config, dict):
        raise ValueError("config.yaml must contain a mapping")
    return config
