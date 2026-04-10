import re
import json
from io import StringIO
from pathlib import Path

from settings.config import MAJOR, MINOR

def newest_file(path: str, file_format: str = 'json'):
    folder = Path(path)

    files = list(folder.rglob(f'*.{file_format}'))

    file = max(
        files,
        key=lambda f: f.stat().st_mtime,
        default=None
    )

    return file.name if file else None
def next_version(filename: str):
    match = re.search(r'v_(\d+_\d+_\d+)', filename)

    if not match:
        pass

    version = match.group(1)
    major, minor, patch = map(int, version.split("_"))

    new_version = f'{MAJOR}_{MINOR}_{patch + 1}'  # retorna patch + 1

    return new_version

def jsonl_to_json(text):
    '''
    Converte dados no formato JSONL (uma linha = um registro JSON) em uma lista de dicionários.

    Cada linha do JSONL é transformada em um dicionário Python, e todos os registros
    são combinados em uma lista única, equivalente a um JSON tradicional. 

    Útil para contornar limitações de APIs que retornam grandes volumes de dados
    em múltiplos batches, evitando erros de parsing como JSONDecodeError.
    '''
    data = []

    # transforma string em objeto similar a file para iterar linhas
    f = StringIO(text)

    with f:
        for line in f:
            line = line.strip()
            if line:  # ignora linhas vazias
                data.append(json.loads(line))

    return data