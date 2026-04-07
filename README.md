# Pipeline ETL Batch para E-commerce

Este projeto implementa um pipeline ETL (Extract, Transform, Load) completo em formato batch para um cenário de e-commerce simulado. O projeto envolve a geração de dados sintéticos, processamento analítico utilizando DuckDB e a geração de insights de negócio.

## 📋 Descrição do Projeto

O sistema foi desenhado para simular o ciclo de vida completo dos dados em um e-commerce:
1. **Geração de Dados**: Criação de dados fictícios de clientes, produtos, pedidos e itens de pedidos usando a biblioteca `Faker`.
2. **Pipeline ETL**: Extração dos dados brutos (arquivos CSV), transformação (limpeza de valores nulos, conversão de tipos, validações) e carga em num banco de dados multicamadas focado em análise (DuckDB).
3. **Análise de Dados**: Execução de consultas SQL na camada analítica do banco de dados para responder a perguntas de negócio e gerar um relatório.

## 🗂️ Estrutura de Arquivos

* `generate_data.py`: Script para gerar os dados brutos fictícios. Ele cria a pasta `data/raw/` (se não existir) e popula ela com os CSVs: `customers.csv`, `products.csv`, `orders.csv` e `order_items.csv`.
* `etl_pipeline.py`: Script principal do pipeline. Extrai os CSVs, processa as camadas Raw -> Treated -> Analytics e armazena os dados consolidados no banco local `warehouse.duckdb`. As execuções são registradas em `etl_pipeline.log`.
* `run_analysis.py`: Lê a camada analítica (`analytics_sales`) no DuckDB para gerar um relatório de faturamento, quantidade de pedidos e top produtos. O resultado é salvo em `analysis_results.txt`.
* `requirements.txt`: Dependências do projeto (Faker, pandas, duckdb).

## 🚀 Como Executar

### 1. Instalação das Dependências

Certifique-se de ter o Python instalado. É recomendado o uso de um ambiente virtual (`venv`).
Para instalar os pacotes necessários, rode o seguinte comando no terminal:

```bash
pip install -r requirements.txt
```

### 2. Passo a Passo do Pipeline

A execução do pipeline deve seguir a seguinte ordem cronológica:

**Passo 1: Gerar os Dados Brutos**
Execute o script para criar a massa de dados sintéticos na pasta `data/raw/`:
```bash
python generate_data.py
```

**Passo 2: Ingestão e Processamento (ETL)**
Rode o fluxo de Extração, Transformação e Carga:
```bash
python etl_pipeline.py
```
Isso criará (ou substituirá) o arquivo de banco de dados `warehouse.duckdb` e gerará os logs da operação.

**Passo 3: Geração de Insights Analíticos**
Após o banco estar preenchido pela camada analítica, execute a análise de resultados:
```bash
python run_analysis.py
```
Esse script exibirá os resultados no terminal e os salvará em um arquivo chamado `analysis_results.txt`.

## 🏗️ Arquitetura de Dados no DuckDB

O arquivo `warehouse.duckdb` é preenchido através de 3 camadas virtuais simuladas em tabelas distintas:
* **Camada Raw**: Tabelas com prefixo `raw_` espelhando os arquivos CSV exatos.
* **Camada Treated**: Tabelas com prefixo `tr_` após os dados passarem por rotinas de limpeza, tratamento de valores nulos e validação de quantidade.
* **Camada Analytics**: Uma tabela flat consolidada chamada `analytics_sales` construída via JOIN de todas as tabelas tratadas, agilizando consultas complexas de negócio.

## 📊 Relatórios e Outputs Gerados
- Logs de Execução ETL: `etl_pipeline.log`
- Evidências da Análise SQL: `analysis_results.txt`
- Banco Local Analítico: `warehouse.duckdb`
