import os
from dotenv import load_dotenv

load_dotenv()

# status de run
TESTE = bool(os.getenv('TEST'))

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