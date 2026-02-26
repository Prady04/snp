import json
import os

from config import DATA_DIR


class StateManager:
    """
    Persists the last-downloaded date for every symbol in a JSON file.
    Writes are atomic (write-to-tmp then rename) to survive crashes.
    """

    FILENAME = "download_state.json"

    def __init__(self, data_dir: str = DATA_DIR):
        self.path   = os.path.join(data_dir, self.FILENAME)
        self._state = self._load()

    def _load(self) -> dict:
        if os.path.exists(self.path):
            with open(self.path) as f:
                return json.load(f)
        return {}

    def get_last_date(self, symbol: str) -> str | None:
        return self._state.get(symbol)

    def set_last_date(self, symbol: str, date_str: str):
        self._state[symbol] = date_str

    def save(self):
        tmp = self.path + ".tmp"
        with open(tmp, "w") as f:
            json.dump(self._state, f, indent=2)
        os.replace(tmp, self.path)
