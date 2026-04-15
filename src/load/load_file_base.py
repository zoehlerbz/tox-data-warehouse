import os
import json

from datetime import datetime
from typing import Optional

from settings.config import TESTE

class LoadFileBase:
    def __init__(self, base_path: str, source: str, entity: str, layer: str, version: str, file_format: str, subfolder_gold: Optional[str] = None) -> None:
        self.base_path = base_path if not TESTE else os.path.join(base_path, 'test')
        self.source = source
        self.entity = entity
        self.layer = layer
        self.version = version
        self.file_format = file_format
        self.subfolder_gold = subfolder_gold
        self.timestamp = datetime.now()

    def _build_file_name(self):
        if self.subfolder_gold == 'kaggle':
            file_name = f'{self.source}_compounds_toxicity_dataset'
        else:
            file_name = f'{self.layer}_{self.source}_{self.entity}'
        return file_name

    def _build_file_path(self, logging: bool = False):

        year = str(self.timestamp.year)
        month = str(self.timestamp.month).zfill(2)
        day = str(self.timestamp.day).zfill(2)

        if self.subfolder_gold is not None:
            folder = os.path.join(self.base_path, self.subfolder_gold)
        elif logging:
            folder = os.path.join(self.base_path, 'logging', year, month, day)
        else:
            folder = os.path.join(self.base_path, year, month, day)
    
        os.makedirs(folder, exist_ok=True)

        file_name = self._build_file_name()

        if self.subfolder_gold is not None:
            full_path = os.path.join(
                folder,
                f"{file_name}.{self.file_format}"
            )
        else:
            full_path = os.path.join(
                folder,
                f"{file_name}_v_{self.version}.{self.file_format}"
            )

        return full_path

    def get_file_path(self):
        return self._build_file_path()

    def load(self, data, logging: bool = False):
        file_path = self._build_file_path(logging)

        try:
            if self.file_format == 'json':
                with open(file_path, 'w', encoding='utf-8') as f:
                    json.dump(data, f, ensure_ascii=False, indent=2)
            elif self.file_format == 'parquet':
                data.to_parquet(file_path, index=False)
            elif self.file_format == 'csv':
                data.to_csv(file_path, index=False)
            else:
                raise ValueError(f'Formato de arquivo não suportado: {self.file_format}')

            print(f'Arquivo armazenado com sucesso: {file_path}')

        except Exception as exc:
            raise RuntimeError(f'Erro ao salvar arquivo em {file_path}') from exc