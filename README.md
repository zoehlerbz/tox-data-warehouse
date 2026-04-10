# 🧪 PubChem Toxicity Data Pipeline

Pipeline de engenharia de dados para coleta, transformação e publicação (ETL) de dados de toxicidade de compostos químicos a partir do PubChem, com disponibilização final em formato `.csv` para uso público no Kaggle.

---

## 🎯 Objetivo

Construir um pipeline reprodutível que:

* realiza web scraping de dados de compostos químicos
* extrai informações de toxicidade
* limpa e estrutura os dados
* gera um dataset confiável
* publica automaticamente no Kaggle

---

## 🧠 Arquitetura do Projeto

O pipeline segue uma abordagem simplificada inspirada na arquitetura medalhão:

```text
[Web Scraping] → [Bronze (raw JSON)] → [Silver (clean Parquet)] → [CSV final] → [Kaggle]
```

---

## ⚙️ Como o Pipeline Funciona

### 1. 🔎 Extração (Scraping)

* Coleta dados do PubChem a partir de uma lista de CIDs
* Extrai:

  * informações do composto (cid, nome, SMILES, InChI, etc.)
  * dados de toxicidade (dose, organismo, rota, etc.)

📦 Saída:

```text
data/bronze/.../bronze_pubchem_id_v_X_X_X.json
data/bronze/.../bronze_pubchem_tox_v_X_X_X.json
```

---

### 2. 🥉 Bronze (dados brutos)

* Dados armazenados **sem alteração**
* Servem como fonte de verdade e reprocessamento

---

### 3. 🥈 Transformação (Silver)

* Limpeza e padronização dos dados
* Conversão de tipos
* Parsing de campos complexos (ex: `"1715 mg/kg"` → `dose_value`, `dose_unit`)

📦 Saída:

```text
data/silver/.../silver_pubchem_id_v_X_X_X.json
data/silver/.../silver_pubchem_tox_v_X_X_X.json
```

---

### 4. 📊 Geração do Dataset Final

* Junção e filtragem dos dados
* Exportação para CSV

📦 Saída:

```text
data/gold/kaggle/pubchem_compounds_toxicity_dataset.csv
```

---

### 5. 📤 Upload para Kaggle

* Dataset publicado automaticamente via API do Kaggle: [In vivo toxicity dataset](https://www.kaggle.com/datasets/bzoehler/in-vivo-toxicity-dataset)

---

Este conjunto de dados foi criado para o desenvolvimento de modelos QSAR (Relação Quantitativa Estrutura–Atividade), particularmente modelos preditivos de toxicidade baseados na estrutura molecular.

Fluxos de trabalho típicos incluem:

* geração de impressões digitais moleculares

* análise de similaridade molecular

* agrupamento de compostos

* predição de toxicidade com aprendizado de máquina

# Tutoriais Relacionados

Alguns tutoriais que podem ajudar ao trabalhar com este conjunto de dados:

* [Manipulação de moléculas com RDKit em Python para modelos de IA](https://zoehlerbz.medium.com/manipulation-of-molecules-with-rdkit-in-python-for-ai-models-8023f1e677c7)

* [Representação de impressões digitais moleculares com Python e RDKit para modelos de IA](https://zoehlerbz.medium.com/representation-of-molecular-fingerprints-with-python-and-rdkit-for-ai-models-8b146bcf3230)

* [Agrupamento de moléculas usando o algoritmo Butina](https://zoehlerbz.medium.com/molecule-clustering-using-the-butina-algorithm-application-with-python-and-rdkit-ffb5b5721447)

* [Aplicação de redes neurais com Python e RDKit para predição de toxicidade de fármacos](https://zoehlerbz.medium.com/neural-network-application-with-python-and-rdkit-for-drug-toxicity-prediction-9acda4685273)

* [Entendendo a variabilidade biológica e seu impacto em modelos preditivos](https://medium.com/@zoehlerbz/understanding-biological-variability-and-its-impact-on-predictive-models-74e7f84be559)

## Dados brutos

Se você tiver interesse em acessar os **dados brutos originais** utilizados para construir este conjunto de dados, fique à vontade para entrar em contato comigo [no LinkedIn](https://www.linkedin.com/in/bernardo-zoehler-94a503212/)