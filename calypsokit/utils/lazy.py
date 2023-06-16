import importlib
from collections.abc import Mapping
from types import ModuleType


class LazyLoader(ModuleType):
    """Module-level lazy loader

    Examples
    --------
    >>> heavy_module = LazyLoader('heavy_module', globals(), 'pkg.heavy_module')
    >>> heavy_module.a
    Attribute a

    """
    def __init__(
        self, local_name: str, parent_module_globals: Mapping, name: str
    ) -> None:
        self._local_name = local_name
        self._parent_module_globals = parent_module_globals
        super().__init__(name)

    def __getattr__(self, attr) -> ModuleType:
        module = self._load()
        return getattr(module, attr)

    def __dir__(self) -> list:
        module = self._load()
        return dir(module)

    def _load(self):
        module = importlib.import_module(self.__name__)
        self._parent_module_globals[self._local_name] = module
        self.__dict__.update(module.__dict__)
        return module
