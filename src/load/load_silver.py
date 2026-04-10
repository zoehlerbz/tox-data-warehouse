from settings.config import SILVER_PATH
from src.load.load_file_base import LoadFileBase

class LoadSilver(LoadFileBase):
    def __init__(self, entity: str, version: str) -> None:
        super().__init__(
            base_path = SILVER_PATH, 
            source = 'pubchem',
            entity = entity,
            layer = 'silver',
            version = version, 
            file_format = 'parquet'
        )

    def load_silver(self, data):
        self.load(data)