import io
import os
import re
import json
import kaggle
import pandas as pd
from datetime import datetime
from zoneinfo import ZoneInfo

from settings.config import KAGGLE_DATASET

class LoadKaggleDataset:

    def __init__(self, dataset_path: str, version: str, extract_start, extract_end) -> None:
        self.dataset_path = dataset_path
        self.dir_path = self._get_dir_path(dataset_path)
        self.version = version.replace('_', '.')
        self.extract_start = str(extract_start)
        self.extract_end = str(extract_end)
        self.extract_time = extract_end - extract_start

    def _get_dataset_info(self):
        dataframe = pd.read_csv(self.dataset_path)

        # rows and columns
        rows = dataframe.shape[0]
        cols = list(dataframe.columns)

        # dataframe.info()
        buffer = io.StringIO()
        dataframe.info(buf=buffer)
        info = "\n".join(
            [f"{col}: {dtype} |" for col, dtype in dataframe.dtypes.items()]
        )

        dataset_infos = {
            'rows': rows,
            'cols': cols,
            'info': info
        }

        return dataset_infos

    def _get_dir_path(self, dataset_path: str):
        return os.path.dirname(dataset_path)

    def _get_metadata(self):
        kaggle.api.dataset_metadata(KAGGLE_DATASET, path=self.dir_path)

    def _update_last_release(self):
        try:
            self._get_metadata()
            with open(f'{self.dir_path}/dataset-metadata.json', 'r') as f:
                metadata_json = f.read()
                
            metadata = json.loads(metadata_json)
                        
            now = datetime.now(ZoneInfo("America/Sao_Paulo"))
            today = now.strftime('%B %d, %Y at %H:%M (UTC%z)')

            description = re.sub(
                r'Last update: [^\n]*',
                f'Last update: {today}',
                metadata['info']['description']
            )

            new_metadata = {
                'id': metadata['info']['datasetSlug'],
                'id_no': metadata['info']['datasetId'],
                'subtitle': metadata['info']['subtitle'],
                'description': description,
            }

            with open(f'{self.dir_path}/dataset-metadata.json', 'w') as f:
                f.write(json.dumps(new_metadata))
            print("Metadados 'dataset-metadata.json' atualizados com sucesso.")
        except Exception as exc:
            raise RuntimeError(f"Erro em obter metadados do dataset Kaggle: {exc}") from exc

    def _get_notes(self):
        dataset_infos = self._get_dataset_info()

        notes = f"""
            [Update - internal v{self.version}][{self.extract_start}] - Monthly full load
            
            Execution:
            - Start: {self.extract_start}
            - End: {self.extract_end}

            Load type:
            - Full load

            Data:
            - Total records: {dataset_infos['rows']}
            - Columns: {dataset_infos['cols']}

            Schema (df.info):
            {dataset_infos['info']}
        """

        return notes

    def load_kaggle(self):
        # Atualiza 'Last update' da descrição da dataset Kaggle
        self._update_last_release()

        notes = self._get_notes()

        abs_path = os.path.abspath(self.dir_path)

        try:
            kaggle.api.dataset_create_version(abs_path, version_notes=notes)
            print(f'Arquivo enviado com sucesso para Kaggle.')
        except Exception as exc:
            raise RuntimeError(f"Erro ao atualizar dataset Kaggle: {exc}") from exc