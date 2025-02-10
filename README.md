

# Desafio final - Programa Lighthouse

Este repositório contém o projeto resultante do desafio final da trilha de engenharia de dados do Programa Lighthouse, desenvolvido pelo aluno Bruno Gustavo Vieira.

O projeto é constituído de um fluxo de ingestão de dados, construído como um job do Databricks, assim como da infraestrutura necessária para a execução deste job.

A infraestrutura é provisionada por um [asset bundle](https://docs.databricks.com/en/dev-tools/bundles/index.html) do Databricks e um projeto Terraform.

### Sumário

- [Pipeline](#pipeline)
- [Job do Databricks](#databricks-job)
- [Convenções de transformação](#transformation-conventions)
- [Parâmetros do job](#job-parameters)
- [Zona de controle](#control-zone)
- [Setup](#setup)
- [Execução](#running)
- [API da zona de controle](#control-zone-api)

<div id="pipeline"></div>

## Pipeline

A fonte de dados é o banco [AdventureWorks](https://learn.microsoft.com/en-us/sql/samples/adventureworks-install-configure?view=sql-server-ver16&tabs=ssms), hospedado em um SQL Server remoto. Os dados do esquema *sales* devem ser movidos para uma **zona raw** e uma **zona de staging** no workspace do Databricks fornecido, com as devidas tranformações sendo aplicadas entre estas duas camadas.

```mermaid
flowchart LR
    s1[fonte de dados]-->|ingestão de dados| d1[zona raw]
    subgraph Databricks
    d1[zona raw]-->|transformações| d2[zona de staging]
    end
    subgraph SQL Server
    s1[fonte de dados]
    end
```

O fluxo se classifica como uma pipeline ELT, pois o carregamento dos dados no destino (Databricks) ocorre antes de transformações serem aplicadas. Conforme representado no fluxograma acima, podemo distinguir três camadas:

**1. Fonte de dados:** Contém os dados originais a serem carregados no Databricks.

**2. Zona raw:** A primeira camada de dados carregados no Databricks. Contém dados idênticos, ou tão próximos quanto possível, àqueles extraídos da fonte. 

**3. Zona de staging:** Contém dados com transformações leves, a fim de proporcionar uma base segura para posteriores modelagem e análise.

<div id="databricks-job"></div>

## Job do Databricks

Na prática, a pipeline é executada pelo job **bruno_vieira_adw_job** do Databricks, que possui a seguinte estrutura de tasks:

```mermaid
graph LR
    t1[init]-->t2[mssql_to_raw]-->t3[raw_to_stg]
```

### init
 Inicializa o job, criando (caso ainda não existam) esquemas e tabelas necessárias para sua execução, e também populando tabelas com um conjunto de registros iniciais.

### mssql_to_raw
Move os dados da fonte para a zona raw. Os dados da zona raw devem ser carregados em um catálogo exclusivo no Unity Catalog, definido pelo parâmetro **raw_catalog_name** do job. Por padrão, será utilizado o catálogo **bruno_vieira_raw**.

### raw_to_stg

Move os dados da zona raw para a zona de staging, aplicando transformações quando necessário. Assim como com a zona raw, esses dados devem ser carregados em um catálogo exclusivo, definido pelo parâmetro **stg_catalog_name**. Por padrão, será utilizado  o catálogo **bruno_vieira_stg**.

**Todas as tabelas** serão submetidas a pelo menos um processo de transformação: os nomes das colunas serão adequados à convenção de nomenclatura *snake_case*, afim de se assegurar essa padronização na base de staging. Esta transformação é aplicada programaticamente a todas as tabelas ingeridas e portanto não é necessário nenhum passo adicional ao incluir novos recursos no [**controle de ingestão**](#ingestion-control).

Outra transformação que será aplicada automaticamente às tabelas é a renomeação das colunas que formam sua chave primária para os nomes definidos na coluna **stg_primary_key** do controle de ingestão, *caso esta coluna não seja nula*. De preferência, [seguir as convenções para nomes de chave primária](#primary-key-name-conventions).

Os nomes das tabelas na zona de staging devem ser definidos pelo usuário no notebook **raw_to_stg.ipynb**, [seguindo, de preferência, as convenções de nomes de tabela](#table-name-conventions). Para aplicar demais transformações, tais como renomeações de colunas, conversões de tipos, entre outras, siga as instruções descritas no notebook. 

Assim como os nomes de colunas, os nomes de esquemas na zona de staging também se adequarão automaticamente à nomenclatura snake_case.

<div id="transformation-conventions"></div>

## Convenções de transformação

<div id="table-name-conventions"></div>

### Nomes de tebela

Recomenda-se, na zona de staging, utilizar nomes de tabela no plural e com nomenclatura snake_case. Por exemplo, se o nome da tabela na fonte é *CreditCard*, utilizar o nome *credit_cards* na zona de staging.

<div id="primary-key-name-conventions"></div>

### Chaves primárias

Se a chave primária na fonte é formada por uma única coluna e referencia-se diretamente à tabela (i.e., não é uma chave estrangeira), recomenda-se utilizar simplesmente "id" na zona de staging ao invés de um nome composto.

Por exemplo, se na origem temos uma tabela *CurrencyRate* com chave primária *CurrencyRateID*, renomear a coluna para *id* na zona de staging.

<div id="job-parameters"></div>

## Parâmetros do job
É possível [executar o job com parâmetros personalizados](#run-job-instructions). Isto será útil em diferentes situações, como por exemplo para executar a pipeline com uma *run_date* diferente da data atual, ou para guardar os dados ingeridos em catálogos diferentes dos de produção em um cenário de depuração.

| Parâmetro | Descrição | 
| ------------ | -------- | 
| ctr_catalog_name | Nome do catálogo da zona de controle. | 
| raw_catalog_name | Nome do catálogo da zona raw. |
| stg_catalog_name | Nome do catálogo da zona de staging. | 
| mssql_host | Host do SQL Server. | 
| mssql_port | Porta do SQL Server. | 
| secret_scope | O escopo dos segredos do Databricks. |
| run_date | A data de execução, em formato ISO. As tabelas com carga incremental serão carregados a partir do **dia anterior** à data fornecida. Por padrão, recebe o valor da data atual. | 
| managed_location | O caminho do local de armazenamento gerenciado. | 

<div id="control-zone"></div>

## Zona de controle

Além dos catálogos para as zonas de raw e de staging, é necessário também um catálogo para a **zona de controle**, responsável por controlar e monitorar a ingestão de tabelas. Este catálogo é definido pelo parâmetro **ctr_catalog_name** do job. Por padrão, será utilizado o catálogo **bruno_vieira_ctr**.

Caso as tabelas da zona de controle ainda não existam durante a execução do job, elas serão criadas na task **init**.

<div id="ingestion-control"></div>

### loading.data_ingestion

A ingestão de dados é realizada em grande parte de forma automatizada. A tabela **loading.data_ingestion** é responsável pelo controle de ingestão.  Nela, definimos quais esquemas e tabelas serão extraídos da fonte, e fornecemos informações que serão necessárias no processo de carregamento. Dessa forma, o utilizador pode com facilidade incluir, excluir ou modificar recursos a serem ingeridos, e essas alterações serão refletidas na próxima execução do job.

| Coluna | Tipo | Descrição | Obrigatória |
| -------- | --------|------------- | ------------ |
| schema_name | string | O nome do esquema que contém a tabela a ser ingerida. | Sim 
| table_name | string | O nome da tabela a ser ingerida. | Sim
| primary_key | string | As colunas que formam a chave primária, separadas por vírgula. | Sim
| stg_primary_key | string | O nome das chaves primárias na zona de staging. Deve conter a mesma quantidade de itens, separados por vírgula, que a coluna *primary_key*.
| active | boolean | Habilita ou desabilita a ingestão da tabela. | Sim
| filter | string | Expressão SQL com a condição que será aplicada à consulta de cargas incrementais, ou nulo para ingestão *full*.
| selected | string | Colunas a serem selecionadas, ou nulo para extração de todas as colunas.
| partition_column | string | A coluna utilizada no particionamento na extração. Nulo para extração não particionada.
| num_partitions | int | Número de partições.

### loading.data_ingestion_log

A tabela loading.data_ingestion_log guardará o histórico de todas as ingestões realizadas, com as seguintes informações:

| Coluna | Tipo | Descricao |
| --------- | ----------| --------- |
| source_catalog_name | string | O nome do catálogo de origem (se existir).
| source_schema_name | string | O nome do esquema de origem.
| source_table_name | string | O nome da tabela de origem.
| target_catalog_name | string | O nome do catálogo de destino.
| target_schema_name | string | O nome do esquema de destino.
| target_table_name | string | O nome da tabela de destino.
| ingestion_date | timestamp | A data da ingestão.
| movements | integer | O número de movimentos (linhas selecionadas pela consulta).
| error |  string | A descrição do erro, caso tenha ocorrido algum.

<div id="setup"></div>

## Setup

### Databricks PAS

Para gerar um token de acesso pessoal (PAS) do Databricks, siga as instruções [nesse link](https://docs.databricks.com/en/dev-tools/auth/pat.html).

### Terraform

Os catálogos base para execução da pipeline são provisionados pelo projeto do Terraform no diretório *./terraform* neste repositório. Para aplicar no workspace a infraestrutura descrita no projeto, é necessário primeiro instalar o Terraform. Para isso, siga as instruções [nesse link](https://developer.hashicorp.com/terraform/tutorials/aws-get-started/install-cli) de acordo com seu sistema operacional.

Após a instalação, podemos prosseguir. Estando na raiz do repositório, devemos nos mover para o diretório do projeto do Terraform:

```bash
cd terraform
```
Dentro desta pasta, crie um [arquivo de variáveis do Terraform](https://developer.hashicorp.com/terraform/language/values/variables#variable-definitions-tfvars-files) nomeado databricks.tfvars. Este arquivo deverá conter as seguintes atribuições de variáveis:

```
databricks_host = <url_do_seu_host>
databricks_token = <seu_token_de_acesso>
```

Após ter atribuido as variáveis corretamente no arquivo .tfvars, podemos criar um [plano de execução do Terraform](https://developer.hashicorp.com/terraform/cli/commands/plan) e exportá-lo.

``` bash
terraform plan -var-file="databricks.tfvars" -out=tfplan
```
Após analisar o plano, o aplicamos ao workspace do Databricks.

```bash
terraform apply tfplan
```
Para confirmar se o workspace foi configurado corretamente, confira se os catálogos **bruno_vieira_ctr**, **bruno_vieira_raw** e **bruno_vieira_stg** foram criados.

### Asset bundle

O deploy do asset bundle é feito de maneira automatizada por uma pipeline do GitHub Actions. Para sua execução, basta que os [secrets do GitHub Actions](https://docs.github.com/en/actions/security-for-github-actions/security-guides/using-secrets-in-github-actions) DATABRICKS_HOST e DATABRICKS_TOKEN estejam atribuídos.

- Quando um push na branch **develop** for identificado, o deploy será realizado para o ambiente **dev** (desenvolvimento).
- Quando um push na branch **main** for identificado, o deploy será realizado para o ambiente **prod** (produção).

**Observação: Caso o deploy do bundle tenha sido feito para ambos os ambientes, duas cópias do job serão criadas no workspace.** O job do ambiente de desenvolvimento pode ser identificado pelo prefixo *[dev <nome_de_usuario>]*.

### Segredos do Databricks

É necessária a atribuição de alguns segredos do Databricks, no escopo definido pelo parâmetro **secret_scope** do job. Por padrão, este escopo será o **bruno_vieira_adw**.

| Secret | Descrição |
| -------- | --------- |
| mssql_username | Usuário do banco SQL Server
| mssql_password | Senha do usuário no banco SQL Server

Os segredos podem ser gerenciados pelo Databricks CLI ou pela UI. As instruções para o gerenciamento de segredos podem ser encontradas [aqui](https://docs.databricks.com/en/security/secrets/index.html).

<div id="running"></div>

## Execução

O job do ambiente  de produção será iniciado uma vez por dia, de forma automática, às 06:00 no horário de Brasília. O job do ambiente de desenvolvimento deve ser iniciado manualmente pelo usuário.

<div id="run-job-instructions"></div>

Para iniciar o job imediatamente pela UI do Databricks, siga as instruções [nesse link](https://learn.microsoft.com/en-us/azure/databricks/jobs/run-now#run-a-job-immediately). É possível também iniciá-lo com parâmetros personalizados, como descrito [aqui](https://learn.microsoft.com/en-us/azure/databricks/jobs/run-now#run-a-job-with-different-parameters).

<div id="control-zone-api"></div>

## API da zona de controle

Uma API opcional está disponível para interagir com a **[zona de controle](#control-zone)**.  Para servir a API localmente, é necessário o [Python 3](https://www.python.org/). Tendo-o instalado, siga as instruções:

A partir da raiz do repositório, navegue até a pasta da API:

```bash
cd api
```

Crie um ambiente virtual do Python e o inicialize.

```bash
python3 -m venv .venv
source .venv/bin/activate
```

Crie um arquivo **.env** com as seguintes variáveis:

```bash
WAREHOUSE_ID = <warehouse_id>
DATABRICKS_HOST = <your_databricks_host>
```

Instale os pacotes necessários.

```bash
pip install -r requirements.txt
```

Navegue para o diretório *src* e inicie o servidor Uvicorn.

```bash
cd src
uvicorn main:app
```

Com o servidor inicializado, navegue para http://127.0.0.1:8000/docs para acessar a UI do Swagger. Os seguintes endpoints estarão disponíveis:

- **GET /**
Busca informações das tabelas sendo ingeridas.

- **GET /logs**
Busca histórico de logs de todas as ingestões de dados realizadas.

- **PATCH {schema_name}/{table_name}/activate**
Ativa a ingestão da tabela especificada.

- **PATCH {schema_name}/{table_name}/activate**
Desativa a ingestão da tabela especificada.
