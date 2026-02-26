from abc import ABC, abstractmethod


class SymbolProvider(ABC):
    """Returns a list of ticker symbols to download."""

    @abstractmethod
    def get_symbols(self) -> list[str]: ...
