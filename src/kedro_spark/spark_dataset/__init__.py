from typing import Any
import lazy_loader as lazy


DynamicFilterDataset: Any
DynamicIcebergDataset: Any

__getattr__, __dir__, __all__ = lazy.attach(
    __name__,
    submod_attrs={
        "dynamic_filter": ["DynamicFilterDataset"],
        "dynamic_iceberg": ["DynamicIcebergDataset"],
    },
)


