import logging

from providers.base import SymbolProvider

log = logging.getLogger(__name__)


class CompositeProvider(SymbolProvider):
    """
    Aggregates symbols from multiple SymbolProviders into a single deduplicated list.
    Order is preserved: symbols from the first provider come first, duplicates
    from later providers are silently dropped.

    Example:
        provider = CompositeProvider([
            SP500Provider(),
            NasdaqProvider(),
            IndexProvider(),
        ])
    """

    def __init__(self, providers: list[SymbolProvider]):
        self._providers = providers

    def get_symbols(self) -> list[str]:
        seen:    set[str]  = set()
        result: list[str] = []
        for provider in self._providers:
            for sym in provider.get_symbols():
                if sym not in seen:
                    seen.add(sym)
                    result.append(sym)
        log.info(f"CompositeProvider: {len(result)} unique symbols from {len(self._providers)} providers")
        return result
