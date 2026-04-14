"""RAI metadata support for Croissant Maker."""

from croissant_baker.rai.injector import inject_rai
from croissant_baker.rai.loader import load_rai_config
from croissant_baker.rai.schema import RAIConfig

__all__ = ["load_rai_config", "inject_rai", "RAIConfig"]
