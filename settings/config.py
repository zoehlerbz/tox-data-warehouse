import os
from dotenv import load_dotenv

load_dotenv()

# status de run
TESTE = bool(os.getenv('TEST') != '0')  # se .env TEST=1, roda o script como teste

# kaggle
if not TESTE:
    KAGGLE_DATASET = os.getenv('KAGGLE_DATASET')
else:
    KAGGLE_DATASET = os.getenv('KAGGLE_DATASET_TEST')

# file path
BRONZE_PATH = './data/bronze/'
SILVER_PATH = './data/silver/'
GOLD_PATH = './data/gold/'

# pubchem
CLASSIFICATION_BASE_URL = 'https://pubchem.ncbi.nlm.nih.gov/classification/cgi/classifications.fcgi'  # url base para obter cache key
ACUTE_EFFECTS_BASE_URL = 'https://pubchem.ncbi.nlm.nih.gov/sdq/sphinxql.cgi?infmt=json&outfmt=jsonl&query='  # url base para obter lista de compostos com Acute Effect --> arquivo JSONL

# postgresql
DB_NAME = os.getenv('DB_NAME')
DB_USER = os.getenv('DB_USER')
DB_PASS = os.getenv('DB_PASS')
DB_HOST = os.getenv('DB_HOST')
DB_PORT = os.getenv('DB_PORT')

# version
MAJOR = 1
MINOR = 0

# data schema
ID_SILVER_SCHEMA = {
    'cid': 'Int64',
    'cmpdname': 'string',
    'iupacname': 'string',
    'inchikey': 'string',
    'inchi': 'string',
    'smiles': 'string'
}

TOX_SILVER_SCHEMA = {
    'cid': 'Int64',
    'sid': 'Int64',
    'organism': 'string',
    'testtype': 'string',
    'route': 'string',
    'dose': 'string'
}

KAGGLE_SCHEMA = {
    'cid':'Int64',
    'name':'string',
    'iupac_name':'string',
    'inchi':'string',
    'inchi_key':'string',
    'smiles':'string',
    'organism':'string',
    'test_type':'string',
    'route':'string',
    'operator':'string',
    'dose_value':'Float64',
    'unit':'string',
    'analyte':'string',
    'complement':'string'
}