from typing import Literal, Optional
from settings.config import GOLD_PATH
from src.load.load_file_base import LoadFileBase

class LoadGold(LoadFileBase):
    def __init__(self, entity: str, version: str, subfolder_gold: Optional[Literal['reporting', 'kaggle', 'ml']], file_format: str) -> None:
        super().__init__(
            base_path = GOLD_PATH, 
            source = 'pubchem',
            entity = entity,
            layer = 'gold',
            version = version, 
            subfolder_gold = subfolder_gold,
            file_format = file_format
        )

    def load_gold(self, data):
        self.load(data)