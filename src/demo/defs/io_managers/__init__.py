import pandas as pd

from dagster import IOManager, io_manager, OutputContext, InputContext
from .parquet import ParquetIOManagerWithQuality
from dagster._core.storage.fs_io_manager import fs_io_manager

ROUTES = {
    "dataframe": ParquetIOManagerWithQuality(),
    "default":  fs_io_manager,
}


class RouterIOManager(IOManager):
    def __init__(self, dataframe_mgr: IOManager, default_mgr: IOManager):
        self._dataframe = dataframe_mgr
        self._default = default_mgr

    def _is_dataframe_output(self, context: OutputContext, obj) -> bool:
        # Prefer runtime check; reliable and simple
        return isinstance(obj, pd.DataFrame)

    def _expects_dataframe_input(self, context: InputContext) -> bool:
        # When loading, decide by the *expected* type if available
        dagster_type = context.dagster_type
        # For Python-annotated types, Dagster exposes typing info on the DagsterType
        py_type = getattr(dagster_type, "typing_type", None)
        return py_type is pd.DataFrame

    def handle_output(self, context: OutputContext, obj):
        if self._is_dataframe_output(context, obj):
            return self._dataframe.handle_output(context, obj)
        return self._default.handle_output(context, obj)

    def load_input(self, context: InputContext):
        if self._expects_dataframe_input(context):
            return self._dataframe.load_input(context)
        return self._default.load_input(context)

@io_manager(required_resource_keys={"io_dataframe", "io_default"})
def router_io_manager(init_context):
    # Expect two inner managers provided as resources
    return RouterIOManager(
        dataframe_mgr=init_context.resources.io_dataframe,
        default_mgr=init_context.resources.io_default
    )


# Helper function to get all IO manager resources automatically
def get_io_manager_resources():
    """Get all configured IO manager resources for use in Definitions"""
    # Convert ROUTES to resource format and add the router
    resources = {f"io_{name}": manager for name, manager in ROUTES.items()}
    resources["io_manager"] = router_io_manager
    return resources


__all__ = ["ParquetIOManagerWithQuality", "router_io_manager", "fs_io_manager", "get_io_manager_resources"]