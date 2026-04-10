from settings.config import BRONZE_PATH
from src.load.load_file_base import LoadFileBase

class LoadBronze(LoadFileBase):
    def __init__(self, entity: str, version: str) -> None:
        super().__init__(
            base_path = BRONZE_PATH, 
            source = 'pubchem',
            entity = entity,
            layer = 'bronze',
            version = version, 
            file_format = 'json'
        )

    def load_bronze(self, data: list):
        self.load(data)
        
    def load_errors(self, data: list):
        self.load(data, logging=True)