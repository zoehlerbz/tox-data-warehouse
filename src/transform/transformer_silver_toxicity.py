import re
import html
import numpy as np
import pandas as pd
from settings.config import TOX_SILVER_SCHEMA

from src.transform.transformer_base import TransformerBase


class TransformerSilverToxicity(TransformerBase):

	"""
	Responsável pela transformação dos dados de toxicidade na camada silver.

	Aplica:
	- Limpeza e padronização estrutural
	- Parsing da dose (valor, operador e unidade)
	- Correção de inconsistências nas unidades
	- Extração de analito e complementos
	- Padronização de unidades para análise comparável

	O objetivo é transformar dados heterogêneos de toxicidade em um formato
	estruturado e consistente para análise posterior.
	"""

	def transform_data(self):

		"""
		Executa o pipeline completo de transformação dos dados de toxicidade.

		Etapas:
		1. Leitura dos dados (bronze)
		2. Limpeza estrutural
		3. Parsing e padronização da dose
		4. Extração de componentes semânticos (analito/complemento)
		5. Padronização de unidades
		6. Remoção de registros inválidos

		Returns:
			pd.DataFrame: Dados de toxicidade transformados.
		"""

		df = self._get_data()

		# Etapa 1: Limpeza estrutural
		df = self._drop_columns(df, ['gid', 'sourceid', 'effect', 'reference'])
		df = self._convert_types(df, TOX_SILVER_SCHEMA)
		df = self._trim_text(df)

		# Etapa 2: Parsing da dose
		df = self._transform_dose_data(df, 'dose')

		# Etapa 3: Correção de inconsistências
		df = self._correct_unit_typos(df)

		# Etapa 4: Extração semântica da unidade
		df = self._get_analyte_and_complement(df)

		# Etapa 5: Padronização de unidades
		df = self._standardize_unit(df)

		# Etapa 6: Limpeza final
		df = self._remove_null(df, ['cid', 'organism', 'testtype', 'route', 'standard_value', 'standard_unit'])

		# Mantém valor original para rastreabilidade
		df.rename(columns={
			'dose': 'raw_dose'
		}, inplace=True)

		return df

	def _transform_dose_data(self, dataframe: pd.DataFrame, dose_col: str):

		"""
		Extrai componentes da dose a partir de string bruta.

		Exemplo:
			">5 mg/kg" → operador=">", value=5, unit="mg/kg"

		Etapas:
		- Separa valor e unidade
		- Remove encoding HTML
		- Extrai operador e valor numérico

		Returns:
			pd.DataFrame
		"""

		dataframe[['dose_', 'unit']] = dataframe[dose_col].str.split(n=1, expand=True)
		dataframe['dose_'] = dataframe['dose_'].apply(html.unescape)

		extracted = dataframe['dose_'].str.extract(r'([^\d]*)(\d*\.?\d+)')

		# Operador (>, <, etc). Default "="
		dataframe['operator'] = extracted[0].replace('', '=')

		# Valor numérico da dose
		dataframe['value'] = pd.to_numeric(extracted[1], errors='coerce')

		return dataframe

	def _correct_unit_typos(self, dataframe: pd.DataFrame):

		"""
		Corrige erros comuns de digitação nas unidades.

		Exemplos:
			'units/k' → 'units/kg'
			'mg(Fe)/k' → 'mg(Fe)/kg'

		Returns:
			pd.DataFrame
		"""

		dataframe['unit'] = dataframe['unit'].replace({
			'units/kg/': 'units/kg',
			'units/k': 'units/kg',
			'mg(Fe)/k': 'mg(Fe)/kg',
			'mg(HAEM)/k': 'mg(HAEM)/kg',
			'mg(base)/k': 'mg(base)/kg',
			'ug(Cd)/k': 'ug(Cd)/kg',
			'ug(Sb)/k': 'ug(Sb)/kg'
		})
		return dataframe

	def _get_analyte_and_complement(self, dataframe: pd.DataFrame):

		"""
		Extrai informações adicionais da unidade:

		- analyte: conteúdo entre parênteses (ex: Fe, Cd, HAEM)
		- complement: partes adicionais da unidade (ex: tempo, condições)

		Também gera 'unit_' limpa para padronização posterior.

		Returns:
			pd.DataFrame
		"""

		dataframe['analyte'] = None
		dataframe['complement'] = None

		for i, row in dataframe.iterrows():

			# Extrai analito entre parênteses
			m = re.search(r'\((.*?)\)', row['unit'])
			if m:
				analito = m.group(1)
				dataframe.at[i, 'analyte'] = analito
			
			# Remove analito da unidade
			unidade_limpa = re.sub(r'\((.*?)\)', '', row['unit'])
			
			# Identifica padrão principal da unidade
			if re.search(r'(/kg|/m3|/cc)', unidade_limpa):

				partes = unidade_limpa.split('/')

				unid = partes[0] + '/' + partes[1]
				dataframe.at[i, f'unit_'] = unid

				if len(partes) > 2:
					complement = '/'.join(partes[2:])
					dataframe.at[i, 'complement'] = complement

			else:

				partes = unidade_limpa.split('/')

				unid = partes[0]
				dataframe.at[i, f'unit_'] = unid

				if len(partes) >= 2:
					complement = partes[1]
					dataframe.at[i, 'complement'] = complement
					
		return dataframe

	def _standardize_unit(self, dataframe):

		"""
		Padroniza unidades para permitir comparabilidade entre registros.

		Conversões suportadas:
		- Massa (/kg, /m3) → mg
		- Volume (/kg) → ml
		- Concentração (ppm, ppb, etc.)
		- Unidades arbitrárias (units)

		Cria:
			- standard_value
			- standard_unit

		Estratégia:
			Caso não seja possível padronizar, mantém valor original (fallback).

		Returns:
			pd.DataFrame
		"""

		unit = dataframe['unit_'].str.lower()

        # Mapas de conversão
		mass_map = {'gm': 1e3, 'mg': 1, 'ug': 1e-3, 'ng': 1e-6, 'pg': 1e-9}
		vol_map = {'ml': 1, 'ul': 1e-3, 'nl': 1e-6}
		ppx_map = {'ppt': 1e6, 'ppb': 1e3, 'ppm': 1, 'pph': 1e-3}
		unit_map = {'ku': 1e3, 'units': 1}

		# MASS /kg
		mass_key = unit.str.extract(r'^(gm|mg|ug|ng|pg)', expand=False)
		mask_mass = unit.str.contains(r'/kg', na=False) & mass_key.notna()

		dataframe.loc[mask_mass, 'standard_value'] = dataframe.loc[mask_mass, 'value'] * mass_key.map(mass_map)
		dataframe.loc[mask_mass, 'standard_unit'] = 'mg/kg'

		# VOLUME /kg
		vol_key = unit.str.extract(r'^(ml|ul|nl)', expand=False)
		mask_vol = unit.str.contains(r'/kg', na=False) & vol_key.notna()

		dataframe.loc[mask_vol, 'standard_value'] = dataframe.loc[mask_vol, 'value'] * vol_key.map(vol_map)
		dataframe.loc[mask_vol, 'standard_unit'] = 'ml/kg'

		# PPX
		ppx_key = unit.str.extract(r'(ppt|ppb|ppm|pph)', expand=False)
		mask_ppx = ppx_key.notna()

		dataframe.loc[mask_ppx, 'standard_value'] = dataframe.loc[mask_ppx, 'value'] * ppx_key.map(ppx_map)
		dataframe.loc[mask_ppx, 'standard_unit'] = 'ppm'

		# MASS /m3
		mask_m3 = unit.str.contains(r'/m3', na=False) & mass_key.notna()

		dataframe.loc[mask_m3, 'standard_value'] = dataframe.loc[mask_m3, 'value'] * mass_key.map(mass_map)
		dataframe.loc[mask_m3, 'standard_unit'] = 'mg/m3'

		# UNITS /kg
		unit_key = unit.str.extract(r'^(ku|units)', expand=False)
		mask_units = unit.str.contains(r'/kg', na=False) & unit_key.notna()

		dataframe.loc[mask_units, 'standard_value'] = dataframe.loc[mask_units, 'value'] * unit_key.map(unit_map)
		dataframe.loc[mask_units, 'standard_unit'] = 'units/kg'

        # Fallback: mantém valor original se não padronizado
		mask_none = dataframe['standard_value'].isna()
		dataframe.loc[mask_none, 'standard_value'] = dataframe.loc[mask_none, 'value']
		dataframe.loc[mask_none, 'standard_unit'] = dataframe.loc[mask_none, 'unit_']

		return dataframe