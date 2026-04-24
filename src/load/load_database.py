import pandas as pd

from sqlalchemy import text
from sqlalchemy import create_engine
from settings.config import DB_USER, DB_PASS, DB_HOST, DB_PORT, DB_NAME

class LoadDatabase:

    def __init__(self, silver_id_path: str, silver_tox_path: str) -> None:
        self.silver_id_path = silver_id_path
        self.silver_tox_path = silver_tox_path
        self.engine = self._postgres_engine()

    def _postgres_engine(self):
        engine = create_engine(
            f"postgresql+psycopg2://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
        )

        return engine
    
    def _prepare_data(self):
        # prepara dados de id
        id = pd.read_parquet(self.silver_id_path)
        id = id[['cid', 'cmpdname', 'iupacname', 'inchi_standardized', 'inchikey_standardized', 'smiles_standardized']]
        id.rename(columns={
            'cid': 'cid', 
            'cmpdname': 'name', 
            'iupacname': 'iupac_name', 
            'inchi_standardized': 'inchi', 
            'inchikey_standardized': 'inchikey', 
            'smiles_standardized': 'smiles'
        }, inplace=True)


        tox = pd.read_parquet(self.silver_tox_path)

        # remove compostos de tox que não estão em id (possivelmente por SMILES incorreto)
        tox = tox[tox["cid"].isin(id["cid"])]

        # renomeia colunas
        tox = tox[['cid', 'organism', 'testtype', 'route', 'operator', 'standard_unit', 'standard_value', 'analyte', 'complement']]
        tox.rename(columns={
            'testtype': 'test_type', 
            'standard_unit': 'unit', 
            'standard_value': 'dose_value', 
        }, inplace=True)

        # garante que não haja valores NaN nas colunas que pode haver nulo --> impede erro de index nas tabelas dimensão
        for col in ['analyte', 'complement']:
            if tox[col].isnull().sum() != 0:
                tox[col] = tox[col].fillna('unknown')

        return id, tox
    
    def _truncate_tables(self):
        tables = ['fact_toxicity', 'dim_organism', 'dim_test_type', 'dim_route', 'dim_operator', 'dim_unit', 'dim_analyte', 'dim_compound']

        with self.engine.begin() as conn:
            for table in tables:
                conn.execute(text(f"TRUNCATE TABLE gold.{table} RESTART IDENTITY CASCADE"))

    def _load_dim_compound(self, dim_compound_data: pd.DataFrame):
        dim_compound_data.to_sql(
            name='dim_compound',
            con=self.engine,
            schema='gold',
            if_exists='append',
            index=False
        )

        with self.engine.connect() as conn:
            result = conn.execute(text("SELECT COUNT(*) FROM gold.dim_compound"))
            print(f'Tabela gold.dim_compound: {result.fetchone()} dados inseridos com sucesso.')

    def _load_dim(self, dim_data: pd.DataFrame, column_name: str, table_name: str):
        dim = (
            dim_data[[column_name]]
            .dropna()
            .drop_duplicates()
        )

        dim.to_sql(
            table_name,
            self.engine,
            schema="gold",
            if_exists="append",
            index=False
        )

        with self.engine.connect() as conn:
            result = conn.execute(text(f"SELECT COUNT(*) FROM gold.{table_name}"))
            print(f'Tabela gold.{table_name}: {result.fetchone()} dados inseridos com sucesso.')

        return pd.read_sql(f"SELECT * FROM gold.{table_name}", self.engine)

    def _prepare_fact(self, tox_data: pd.DataFrame, dim_organism: pd.DataFrame, dim_test_type: pd.DataFrame, dim_route: pd.DataFrame, dim_operator: pd.DataFrame, dim_unit: pd.DataFrame, dim_analyte: pd.DataFrame):
        fact = tox_data.copy()

        fact = fact.merge(dim_organism, on="organism", how="left")
        fact = fact.merge(dim_test_type, on="test_type", how="left")
        fact = fact.merge(dim_route, on="route", how="left")
        fact = fact.merge(dim_operator, on="operator", how="left")
        fact = fact.merge(dim_unit, on="unit", how="left")
        fact = fact.merge(dim_analyte, on="analyte", how="left")

        return fact[[
            "cid",
            "organism_id",
            "test_type_id",
            "route_id",
            "operator_id",
            "unit_id",
            "analyte_id",
            "dose_value",
            "complement"
        ]].copy()
    
    def _load_fact(self, fact_data: pd.DataFrame):
        fact_data.to_sql(
            "fact_toxicity",
            self.engine,
            schema="gold",
            if_exists="append",
            index=False
        )

        with self.engine.connect() as conn:
            result = conn.execute(text(f"SELECT COUNT(*) FROM gold.fact_toxicity"))
            print(f'Tabela gold.fact_toxicity: {result.fetchone()} dados inseridos com sucesso.')

    def load_database(self):
        # prepara dados
        id, tox = self._prepare_data()

        # executa TRUNCATE TABLEs
        self._truncate_tables()

        # load dimensão compounds
        self._load_dim_compound(id)

        # load demais dimensões
        dim_organism = self._load_dim(tox, "organism", "dim_organism")
        dim_test_type = self._load_dim(tox, "test_type", "dim_test_type")
        dim_route = self._load_dim(tox, "route", "dim_route")
        dim_operator = self._load_dim(tox, "operator", "dim_operator")
        dim_unit = self._load_dim(tox, "unit", "dim_unit")
        dim_analyte = self._load_dim(tox, "analyte", "dim_analyte")

        # prepara tabela fact
        fact_toxicity = self._prepare_fact(tox, dim_organism, dim_test_type, dim_route, dim_operator, dim_unit, dim_analyte)

        print(f'Carregamento do banco de dados concluído com sucesso.')