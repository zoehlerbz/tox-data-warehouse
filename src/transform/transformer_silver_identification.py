import pandas as pd
from rdkit import Chem, RDLogger
from rdkit.Chem import SaltRemover
from rdkit.Chem.MolStandardize import rdMolStandardize
from src.transform.transformer_base import TransformerBase
from settings.config import ID_SILVER_SCHEMA

# Desabilita logs verbosos do RDKit (melhora legibilidade do pipeline)
RDLogger.DisableLog('rdApp.*')

class TransformerSilverIdentification(TransformerBase):

    """
    Responsável pela transformação dos dados de identificação de compostos
    na camada silver.

    Aplica:
    - Padronização de tipos e strings
    - Conversão de SMILES para objetos moleculares (RDKit)
    - Geração de identificadores químicos canônicos (SMILES, InChI, InChIKey)
    """

    def transform_data(self):

        """
        Executa o pipeline de transformação dos dados de identificação.

        Etapas:
        1. Leitura dos dados (bronze)
        2. Padronização de tipos e textos
        3. Conversão de SMILES → objeto molecular (Mol)
        4. Geração de identificadores químicos padronizados
        5. Limpeza final (remoção de nulos e colunas auxiliares)

        Returns:
            pd.DataFrame: DataFrame transformado e pronto para camada silver.
        """

        df = self._get_data()

        # Etapa 1: Padronização básica
        df = self._convert_types(df, ID_SILVER_SCHEMA)
        df = self._trim_text(df)

        # Etapa 2: Criação de objeto molecular
        df['mol'] = df['smiles'].apply(self._safe_mol_from_smiles)

        # Etapa 3: Geração de identificadores canônicos
        df['smiles_standardized'] = df['mol'].apply(self._mol_to_smiles)
        df['inchi_standardized'] = df['mol'].apply(self._mol_to_inchi)
        df['inchikey_standardized'] = df['mol'].apply(self._mol_to_inchikey)

        # Etapa 4: Limpeza final
        # Remove objeto molecular intermediário (não serializável / desnecessário)
        df.drop(columns=['mol'], inplace=True)

        # Remove registros sem identificador essencial
        df = self._remove_null(df, ['cid', 'smiles_standardized'])

        return df

    def _safe_mol_from_smiles(self, smiles: str):
        
        """
        Converte uma string SMILES em objeto Mol de forma segura.

        Args:
            smiles (str): Representação SMILES do composto.

        Returns:
            Mol | None: Objeto molecular ou None em caso de falha.
        """

        if pd.isna(smiles):
            return None
        return Chem.MolFromSmiles(smiles)

    def _mol_to_smiles(self, mol):

        """
        Converte objeto Mol para SMILES canônico.

        Returns:
            str | None
        """

        if mol is None:
            return None
        return Chem.MolToSmiles(mol, isomericSmiles=True, canonical=True)

    def _mol_to_inchi(self, mol):

        """
        Converte objeto Mol para InChI.

        Returns:
            str | None
        """

        if mol is None:
            return None
        try:
            return Chem.MolToInchi(mol)
        except Exception:
            return None

    def _mol_to_inchikey(self, mol):
        
        """
        Converte objeto Mol para InChIKey.

        Returns:
            str | None
        """

        if mol is None:
            return None
        try:
            inchi = Chem.MolToInchi(mol)
            return Chem.inchi.InchiToInchiKey(inchi)
        except Exception:
            return None