from dagster import Definitions
from .io_managers import ParquetIOManagerWithQuality, router_io_manager, fs_io_manager, get_io_manager_resources


def get_all(object):
    return [getattr(object, name) for name in getattr(object, "__all__", [])]

defs = Definitions(
    assets=[],
    jobs=[],
    resources={
        # IOManager - automatically loaded from io_managers configuration
        **get_io_manager_resources(),
    }
)