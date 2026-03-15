# lib-spark

Biblioteca PySpark para escrita padronizada em tabelas Iceberg registradas no AWS Glue Catalog.

**Versao:** 0.1.0  
**Ambiente-alvo:** Amazon EMR 7.10.0 | Spark 3.5.5 | Apache Iceberg 1.5.0

---

## Indice

1. [Visao geral](#visao-geral)
2. [Instalacao](#instalacao)
3. [Inicio rapido](#inicio-rapido)
4. [Conceitos principais](#conceitos-principais)
5. [API publica](#api-publica)
   - [GlueTableManager](#gluetablemanager)
   - [WriteConfig](#writeconfig)
   - [WriteOptimizationConfig](#writeoptimizationconfig)
   - [MaintenanceConfig](#maintenanceconfig)
   - [WriteResult](#writeresult)
6. [Operacoes de escrita](#operacoes-de-escrita)
   - [append](#append)
   - [overwrite_table](#overwrite_table)
   - [overwrite_partitions](#overwrite_partitions)
   - [merge](#merge)
7. [Estrategias de carga](#estrategias-de-carga)
   - [full](#full)
   - [incremental](#incremental)
8. [Leitura de tabelas](#leitura-de-tabelas)
9. [Parametros de runtime (DAG)](#parametros-de-runtime-dag)
10. [Otimizacao de escrita](#otimizacao-de-escrita)
11. [Manutencao de tabelas](#manutencao-de-tabelas)
12. [Politicas de schema](#politicas-de-schema)
    - [STRICT](#strict)
    - [SAFE_SCHEMA_EVOLUTION](#safe_schema_evolution)
    - [FAIL_ON_DIFF](#fail_on_diff)
    - [CUSTOM](#custom)
13. [Validacoes automaticas](#validacoes-automaticas)
14. [Auditoria e observabilidade](#auditoria-e-observabilidade)
15. [Tratamento de erros](#tratamento-de-erros)
16. [Arquitetura interna](#arquitetura-interna)
17. [Referencia de configuracao](#referencia-de-configuracao)
18. [Exemplos completos](#exemplos-completos)
19. [Build e distribuicao](#build-e-distribuicao)
20. [Testes](#testes)

---

## Visao geral

A `lib-spark` centraliza a logica de persistencia em tabelas Iceberg no Glue Catalog, oferecendo:

- Interface unica (`GlueTableManager`) para leitura e escrita
- Quatro modos de escrita: append, overwrite_table, overwrite_partitions e merge
- Otimizacao automatica de escrita para reducao de small files (defaults sensiveis, sem configuracao obrigatoria)
- Manutencao opcional pos-escrita: expiracao de snapshots, rewrite de data files e manifests
- Validacao automatica de parametros, schema, chaves e particoes antes de cada escrita
- Evolucao controlada de schema com politicas configuraveis
- Auditoria integrada de cada operacao (duracao, contagem, resultado)
- Mensagens de erro claras e padronizadas

O desenvolvedor informa **o que** deseja fazer; a biblioteca decide **como** executar.
Sem configuracao avancada, a biblioteca ja aplica defaults que ajudam a reduzir small files e manter a performance.

---

## Instalacao

### No EMR (via .whl no S3)

```bash
# 1. Gerar o pacote
python -m build

# 2. Upload para o S3
aws s3 cp dist/lib_spark-0.1.0-py3-none-any.whl s3://seu-bucket/libs/

# 3. Usar no spark-submit
spark-submit --py-files s3://seu-bucket/libs/lib_spark-0.1.0-py3-none-any.whl seu_job.py
```

### Desenvolvimento local

```bash
pip install -e ".[dev]"
```

---

## Inicio rapido

```python
from pyspark.sql import SparkSession
from lib_spark import GlueTableManager, WriteConfig, WriteMode

spark = SparkSession.builder.getOrCreate()

df = spark.createDataFrame([
    (1, "Alice", 100.0),
    (2, "Bob", 200.0),
], ["id", "nome", "valor"])

config = WriteConfig(
    target_table="glue_catalog.meu_database.minha_tabela",
    write_mode=WriteMode.APPEND,
)

manager = GlueTableManager(spark)
result = manager.write(df, config)

print(f"Sucesso: {result.success}")
print(f"Registros escritos: {result.records_written}")
print(f"Duracao: {result.duration_seconds:.2f}s")
```

---

## Conceitos principais

| Conceito | Descricao |
|---|---|
| **WriteMode** | Qual operacao de escrita executar (append, overwrite, merge) |
| **LoadStrategy** | Como tratar os dados de entrada (full ou incremental) |
| **SchemaPolicy** | Como lidar com diferencas entre o schema do DataFrame e o da tabela |
| **WriteConfig** | Objeto que reune todos os parametros de uma operacao de escrita |
| **WriteOptimizationConfig** | Controla otimizacao de escrita (reducao de small files). Opcional, com defaults sensiveis |
| **MaintenanceConfig** | Controla manutencao pos-escrita (snapshots, compactacao). Opcional, desativado por padrao |
| **WriteResult** | Resultado retornado apos cada escrita (sucesso, contagem, duracao, erros) |

---

## API publica

### GlueTableManager

Ponto de entrada principal da biblioteca. Recebe uma `SparkSession` e orquestra todo o fluxo.

```python
from lib_spark import GlueTableManager

manager = GlueTableManager(spark)
```

#### `manager.write(df, config, optimization=None, maintenance=None) -> WriteResult`

Executa uma operacao de escrita validada e auditada.

**Parametros:**

| Parametro | Tipo | Default | Descricao |
|---|---|---|---|
| `df` | `DataFrame` | *obrigatorio* | DataFrame PySpark com os dados a serem escritos |
| `config` | `WriteConfig` | *obrigatorio* | Configuracao completa da operacao |
| `optimization` | `WriteOptimizationConfig \| None` | `None` | Otimizacao de escrita. Se `None`, usa defaults internos (`enabled=True`) |
| `maintenance` | `MaintenanceConfig \| None` | `None` | Manutencao pos-escrita. Se `None`, usa defaults internos (`enabled=False`) |

**Retorno:** `WriteResult`

**Fluxo interno:**

1. Valida parametros do `WriteConfig`, `WriteOptimizationConfig` e `MaintenanceConfig`
2. Resolve metadados da tabela no catalogo (schema, particoes, existencia)
3. Valida DataFrame contra a tabela (chaves, particoes, schema)
4. Compara schemas e aplica politica de evolucao
5. Prepara DataFrame (aplica filtro incremental se necessario)
6. Aplica otimizacao de escrita (distribuicao, reparticionamento, tamanho de arquivo)
7. Executa a escrita com o writer adequado
8. Executa manutencao pos-escrita (se habilitada)
9. Registra auditoria e retorna `WriteResult`

#### `manager.read(table_name, ...) -> DataFrame`

Le uma tabela do catalogo de forma padronizada.

**Parametros:**

| Parametro | Tipo | Obrigatorio | Descricao |
|---|---|---|---|
| `table_name` | `str` | Sim | Nome qualificado da tabela (`catalogo.database.tabela`) |
| `incremental_column` | `str` | Nao | Coluna de controle para leitura incremental |
| `incremental_value` | `Any` | Nao | Valor de corte para filtro incremental |

**Retorno:** `DataFrame`

---

### WriteConfig

Dataclass com todos os parametros de uma operacao de escrita.

```python
from lib_spark import WriteConfig, WriteMode, LoadStrategy, SchemaPolicy

config = WriteConfig(
    target_table="glue_catalog.meu_database.minha_tabela",
    write_mode=WriteMode.MERGE,
    load_strategy=LoadStrategy.INCREMENTAL,
    merge_keys=["id"],
    partition_columns=["ano", "mes"],
    incremental_column="updated_at",
    incremental_value="2024-01-01",
    schema_policy=SchemaPolicy.SAFE_SCHEMA_EVOLUTION,
    extra_options={},
)
```

**Campos:**

| Campo | Tipo | Default | Descricao |
|---|---|---|---|
| `target_table` | `str` | *obrigatorio* | Nome qualificado da tabela destino |
| `write_mode` | `WriteMode` | *obrigatorio* | Operacao de escrita |
| `load_strategy` | `LoadStrategy` | `FULL` | Estrategia de carga |
| `merge_keys` | `List[str]` | `[]` | Chaves para MERGE (obrigatorio se `write_mode=MERGE`) |
| `partition_columns` | `List[str]` | `[]` | Colunas de particao (obrigatorio se `write_mode=OVERWRITE_PARTITIONS`) |
| `incremental_column` | `str \| None` | `None` | Coluna de controle (obrigatorio se `load_strategy=INCREMENTAL`) |
| `incremental_value` | `Any \| None` | `None` | Valor de corte do incremental |
| `schema_policy` | `SchemaPolicy` | `STRICT` | Politica de evolucao de schema |
| `custom_schema_rules` | `Dict \| None` | `None` | Regras customizadas (reservado para `SchemaPolicy.CUSTOM`) |
| `extra_options` | `Dict[str, Any]` | `{}` | Opcoes adicionais para extensibilidade futura |

---

### WriteOptimizationConfig

Controla otimizacoes de escrita para reducao de small files. **Opcional.** Quando nao informado, a biblioteca usa uma instancia padrao com `enabled=True` -- ou seja, a otimizacao funciona automaticamente sem nenhuma configuracao adicional.

```python
from lib_spark import WriteOptimizationConfig

opt = WriteOptimizationConfig(
    target_file_size_mb=256,
    distribution_mode="range",
)
result = manager.write(df, config, optimization=opt)
```

**Campos:**

| Campo | Tipo | Default | Descricao |
|---|---|---|---|
| `enabled` | `bool` | `True` | Ativa ou desativa a otimizacao |
| `distribution_mode` | `str` | `"hash"` | Modo de distribuicao Iceberg: `"none"`, `"hash"` ou `"range"` |
| `sort_columns` | `List[str]` | `[]` | Colunas para ordenacao fisica dos dados (equivalente ao `CLUSTER BY` do BigQuery). Quando informado, aplica `WRITE ORDERED BY` na tabela Iceberg |
| `target_file_size_mb` | `int` | `512` | Tamanho alvo de cada arquivo escrito (em MB) |
| `advisory_partition_size_mb` | `int` | `1024` | Tamanho aconselhado para particoes Spark (em MB) |
| `min_input_files_before_repartition` | `int` | `10` | Numero minimo de particoes do DataFrame para que o reparticionamento seja aplicado |
| `repartition_by_partition_columns` | `bool` | `True` | Se `True`, reorganiza o DataFrame pelas colunas de particao antes da escrita |

**Importante:** `distribution_mode` controla como o Iceberg distribui dados entre arquivos. `"hash"` e o melhor default geral. Use `"range"` para tabelas onde a ordenacao dos dados importa. Use `"none"` apenas se a redistribuicao nao for desejada.

---

### MaintenanceConfig

Controla rotinas de manutencao executadas apos a escrita. **Opcional e desativado por padrao.** Quando nao informado, nenhuma manutencao e executada.

A manutencao e independente da otimizacao de escrita. Otimizacao atua **durante** a escrita (reduce small files no momento da gravacao). Manutencao atua **depois** da escrita (limpa snapshots antigos, compacta arquivos existentes).

```python
from lib_spark import MaintenanceConfig

mnt = MaintenanceConfig(
    enabled=True,
    snapshot_retention_days=15,
    retain_last_snapshots=3,
)
result = manager.write(df, config, maintenance=mnt)
```

**Campos:**

| Campo | Tipo | Default | Descricao |
|---|---|---|---|
| `enabled` | `bool` | `False` | Ativa ou desativa a manutencao pos-escrita |
| `expire_snapshots` | `bool` | `True` | Expira snapshots mais antigos que `snapshot_retention_days` |
| `snapshot_retention_days` | `int` | `30` | Retencao em dias para snapshots |
| `retain_last_snapshots` | `int` | `5` | Numero minimo de snapshots a manter, independentemente da idade |
| `rewrite_data_files` | `bool` | `False` | Compacta data files pequenos (resolve small files historicos) |
| `rewrite_data_files_min_input_files` | `int` | `20` | Numero minimo de arquivos para que o rewrite seja acionado |
| `rewrite_manifests` | `bool` | `False` | Reescreve manifests para otimizar metadados |

**Importante:**
- `expire_snapshots` **nao resolve** small files. Ele remove versoes antigas da tabela (time-travel). Para resolver small files ja existentes, use `rewrite_data_files=True`.
- A otimizacao de escrita (`WriteOptimizationConfig`) previne a criacao de small files novos. A manutencao (`MaintenanceConfig`) corrige small files antigos.

---

### WriteResult

Dataclass retornada apos cada operacao de escrita.

```python
result = manager.write(df, config)

print(result.success)              # True / False
print(result.target_table)         # "glue_catalog.db.tabela"
print(result.write_mode)           # WriteMode.APPEND
print(result.load_strategy)        # LoadStrategy.FULL
print(result.records_written)      # 1500
print(result.schema_changes)       # SchemaDiff ou None
print(result.optimization_applied) # True / False
print(result.maintenance_actions)  # ["expire_snapshots(...)"] ou []
print(result.started_at)           # datetime UTC
print(result.finished_at)          # datetime UTC
print(result.duration_seconds)     # 12.34
print(result.error)                # None ou mensagem de erro
```

**Campos:**

| Campo | Tipo | Descricao |
|---|---|---|
| `success` | `bool` | Se a operacao foi bem-sucedida |
| `target_table` | `str` | Tabela de destino |
| `write_mode` | `WriteMode` | Modo de escrita executado |
| `load_strategy` | `LoadStrategy` | Estrategia de carga utilizada |
| `records_written` | `int` | Quantidade de registros escritos |
| `schema_changes` | `SchemaDiff \| None` | Diferencas de schema detectadas (se houver) |
| `optimization_applied` | `bool` | Se a otimizacao de escrita foi aplicada |
| `maintenance_actions` | `List[str]` | Lista de acoes de manutencao executadas (vazia se nenhuma) |
| `started_at` | `datetime` | Inicio da operacao (UTC) |
| `finished_at` | `datetime` | Fim da operacao (UTC) |
| `duration_seconds` | `float` | Duracao total em segundos |
| `error` | `str \| None` | Mensagem de erro em caso de falha |

---

## Operacoes de escrita

### append

Adiciona novos registros a tabela sem alterar os existentes.

```python
config = WriteConfig(
    target_table="glue_catalog.vendas.fato_vendas",
    write_mode=WriteMode.APPEND,
)
result = manager.write(df, config)
```

**Mecanismo interno:** `df.writeTo(table).append()`

**Quando usar:**
- Ingestao de novos lotes de dados
- Tabelas de log ou eventos
- Cenarios onde nao ha necessidade de atualizar registros

---

### overwrite_table

Substitui completamente todo o conteudo da tabela.

```python
config = WriteConfig(
    target_table="glue_catalog.vendas.dim_produto",
    write_mode=WriteMode.OVERWRITE_TABLE,
)
result = manager.write(df, config)
```

**Mecanismo interno:** `INSERT OVERWRITE TABLE ... SELECT * FROM ...`

**Quando usar:**
- Tabelas dimensao que precisam ser recriadas por completo
- Reprocessamento total
- Tabelas pequenas que sao recalculadas integralmente

---

### overwrite_partitions

Substitui somente as particoes presentes no DataFrame de entrada. Particoes nao presentes nos dados permanecem intactas.

```python
config = WriteConfig(
    target_table="glue_catalog.vendas.fato_vendas",
    write_mode=WriteMode.OVERWRITE_PARTITIONS,
    partition_columns=["ano", "mes"],
)
result = manager.write(df, config)
```

**Mecanismo interno:** `df.writeTo(table).overwritePartitions()` (dynamic overwrite nativo do Iceberg)

**Quando usar:**
- Reprocessamento parcial por particao
- Correcao de dados em periodos especificos
- Carga incremental por janela de tempo

**Requisito:** `partition_columns` deve ser informado e corresponder as colunas de particao da tabela.

---

### merge

Realiza upsert (insert + update) com base em chave(s) definida(s). Registros existentes sao atualizados; novos registros sao inseridos.

```python
config = WriteConfig(
    target_table="glue_catalog.vendas.dim_cliente",
    write_mode=WriteMode.MERGE,
    merge_keys=["id_cliente"],
)
result = manager.write(df, config)
```

**Mecanismo interno:**
```sql
MERGE INTO tabela AS target
USING source_view AS source
ON target.id_cliente = source.id_cliente
WHEN MATCHED THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT *
```

**Quando usar:**
- Tabelas SCD (Slowly Changing Dimensions)
- Dados que podem chegar duplicados ou atualizados
- Tabelas com chave primaria logica

**Requisito:** `merge_keys` deve ser informado. As chaves devem existir tanto no DataFrame quanto na tabela destino.

**Merge com chave composta:**
```python
config = WriteConfig(
    target_table="glue_catalog.vendas.fato_transacoes",
    write_mode=WriteMode.MERGE,
    merge_keys=["id_transacao", "id_loja"],
)
```

---

## Estrategias de carga

A estrategia de carga e separada da estrategia de escrita. Ela define **quais dados** do DataFrame serao considerados, antes de chegar ao writer.

### full

Processa todos os registros do DataFrame. Este e o padrao.

```python
config = WriteConfig(
    target_table="glue_catalog.db.tabela",
    write_mode=WriteMode.APPEND,
    load_strategy=LoadStrategy.FULL,
)
```

### incremental

Filtra o DataFrame mantendo apenas registros onde `incremental_column > incremental_value`.

```python
config = WriteConfig(
    target_table="glue_catalog.db.tabela",
    write_mode=WriteMode.APPEND,
    load_strategy=LoadStrategy.INCREMENTAL,
    incremental_column="updated_at",
    incremental_value="2024-06-01T00:00:00",
)
```

O filtro e aplicado internamente antes da escrita. O DataFrame original nao e modificado.

**Requisitos:**
- `incremental_column` deve ser informado
- `incremental_value` deve ser informado
- A coluna deve existir no DataFrame

---

## Leitura de tabelas

### Leitura completa

```python
df = manager.read("glue_catalog.vendas.fato_vendas")
```

### Leitura incremental

```python
df = manager.read(
    "glue_catalog.vendas.fato_vendas",
    incremental_column="updated_at",
    incremental_value="2024-06-01",
)
# Retorna apenas registros onde updated_at > "2024-06-01"
```

---

## Parametros de runtime (DAG)

Quando o job e orquestrado por uma DAG (ex.: Airflow), parametros como **full_refresh**, **date_start** e **date_end** podem vir do trigger manual. A lib-spark oferece uma camada para validar, normalizar e resolver o modo de execucao, aplicar filtros de data de forma padronizada e fazer `full_refresh` sobrescrever a estrategia incremental **sem que o desenvolvedor escreva** `if full_refresh` ou `if date_start` em cada job.

### Conceitos

| Conceito | Descricao |
|---|---|
| **RuntimeParams** | Parametros brutos vindos da DAG: `full_refresh` (bool ou string), `date_start`, `date_end` (strings ou vazios) |
| **JobExecutionConfig** | Configuracao do job: modo padrao, coluna de data, e quais modos o job suporta (`supports_full_refresh`, `supports_from_date`, `supports_date_range`) |
| **ExecutionContext** | Resultado da resolucao: modo efetivo, `effective_date_start`/`effective_date_end`, flags `is_full_refresh` e `should_apply_date_filter` |
| **ExecutionMode** | Modo de execucao: `FULL_REFRESH`, `INCREMENTAL_DEFAULT`, `FROM_DATE`, `DATE_RANGE` |

### Precedencia do modo

O modo e resolvido na seguinte ordem (o primeiro que se aplicar vence):

| Prioridade | Condicao | Modo |
|---|---|---|
| 1 | `full_refresh=True` (ou string "true"/"1"/"yes") | `FULL_REFRESH` |
| 2 | `date_start` e `date_end` informados | `DATE_RANGE` |
| 3 | Apenas `date_start` informado | `FROM_DATE` |
| 4 | Nenhum parametro (ou defaults) | `INCREMENTAL_DEFAULT` (ou o `default_mode` do `JobExecutionConfig`) |

**Regras para datas:**

- Se `date_end` for informado, `date_start` e obrigatorio.
- `date_start` deve ser menor ou igual a `date_end`.
- Valores vazios ou `None` sao ignorados (sem filtro de data).

### full_refresh e estrategia incremental

Quando o contexto e `FULL_REFRESH`, a biblioteca trata isso de duas formas que o desenvolvedor usa **antes** de chamar `manager.write()`:

1. **Filtro de dados:** `apply_runtime_filter(df, context, date_column)` — em modo FULL_REFRESH ou INCREMENTAL_DEFAULT **nao aplica** filtro; em FROM_DATE aplica `col >= effective_date_start`; em DATE_RANGE aplica o intervalo.
2. **Config de escrita:** `effective_write_config(config, context)` — se `context.is_full_refresh` e True, retorna uma **copia** do `WriteConfig` com `load_strategy=LoadStrategy.FULL` e `incremental_column`/`incremental_value=None`; senao retorna o config inalterado. Assim, o mesmo job pode ser chamado com params de full refresh e a escrita sera full, sem o desenvolvedor checar `if full_refresh` no codigo.

### Parametros via spark-submit (recomendado para DAG)

A DAG pode repassar os parametros para o job via argumentos do spark-submit. O script recebe `--data_inicio`, `--data_fim` e `--full_refresh` e usa `parse_runtime_params_from_argv()` para obter um `RuntimeParams`:

```bash
spark-submit --py-files s3://bucket/libs/lib_spark-0.1.0-py3-none-any.whl \
  s3://bucket/jobs/meu_job.py --data_inicio=2025-05-01 --data_fim=2025-05-31
```

No script, use `parse_runtime_params_from_argv()` (sem argumentos para ler `sys.argv`; ou passe uma lista de strings para testes). Aceita tambem `--date_start` / `--date_end` como alias e `--full-refresh` como alias de `--full_refresh`.

### Como o engenheiro de dados deve construir o script PySpark

Para que o job suporte parametros vindos da DAG (`--data_inicio`, `--data_fim`, `--full_refresh`), o script PySpark precisa seguir um fluxo unico e previsivel. Nao e necessario escrever `if full_refresh` ou `if date_start` manualmente: a lib-spark centraliza essa logica.

**Passos obrigatorios no script:**

1. **Obter a SparkSession** (como ja faz hoje).
2. **Ler os parametros da DAG** — chamar `parse_runtime_params_from_argv()`. Isso le os argumentos que a DAG passou no spark-submit (ex.: `--data_inicio=2025-05-01`) e devolve um `RuntimeParams`. Se o job nao for disparado pela DAG, o spark-submit nao precisa passar esses args; o parser usa defaults (sem data, full_refresh=false).
3. **Definir a configuracao do job** — criar um `JobExecutionConfig` com: `default_mode` (geralmente `INCREMENTAL_DEFAULT`), `date_column` (nome da coluna de data usada no filtro, ex.: `"dt"` ou `"updated_at"`), e os flags `supports_full_refresh`, `supports_from_date`, `supports_date_range` conforme o que o job aceita.
4. **Resolver o contexto** — chamar `ExecutionResolver().resolve(params, job_config)`. O resultado e um `ExecutionContext` (modo efetivo, datas efetivas, flags). Se os params forem invalidos (ex.: data_fim sem data_inicio), o resolver lanca excecao e o job falha de forma controlada.
5. **Ler ou montar o DataFrame** — como hoje (ex.: `spark.sql(...)` ou `manager.read(...)`).
6. **Aplicar o filtro de data** — chamar `apply_runtime_filter(df, context, job_config.date_column)`. Em FULL_REFRESH ou sem datas, nao altera o DataFrame; em FROM_DATE ou DATE_RANGE, aplica o filtro pela coluna de data. O script sempre usa o DataFrame retornado daqui em diante.
7. **Definir o WriteConfig base** — o mesmo que o job usaria sem runtime params (tabela, write_mode, load_strategy, incremental_column, incremental_value, etc.).
8. **Obter o config efetivo** — chamar `effective_write_config(base_config, context)`. Se o contexto for full_refresh, a lib retorna um config com estrategia FULL e sem incremental; senao retorna o config inalterado.
9. **Escrever** — chamar `manager.write(df_filtered, write_config)`.

**Resumo do fluxo no codigo:**

```
params = parse_runtime_params_from_argv()
job_config = JobExecutionConfig(default_mode=..., date_column="dt", supports_...=True)
context = ExecutionResolver().resolve(params, job_config)
df = ...  # leitura/transformacao
df_filtered = apply_runtime_filter(df, context, job_config.date_column)
write_config = effective_write_config(base_write_config, context)
manager.write(df_filtered, write_config)
```

**O que a DAG precisa fazer:** ao montar o comando do step (ex.: EmrAddStepsOperator), incluir nos argumentos do spark-submit os parametros que o usuario informou no trigger, por exemplo: `--data_inicio={{ ti.xcom_pull(...) }}`, `--data_fim=...`, `--full_refresh=...`. O script so precisa chamar `parse_runtime_params_from_argv()` no inicio; nao precisa ler variaveis de ambiente nem outro mecanismo, a menos que a equipe prefira.

**Coluna de data:** o valor de `JobExecutionConfig.date_column` deve ser o nome da coluna do DataFrame que representa a data do registro (ex.: `dt`, `data_ref`, `updated_at`). E essa coluna que `apply_runtime_filter` usa para aplicar `>= date_start` ou o intervalo. Se a tabela for particionada por essa coluna, o filtro tambem reduz dados lidos/escritos.

### Uso tipico

1. Obter parametros da DAG: no script, chamar `params = parse_runtime_params_from_argv()` (argumentos do spark-submit) ou instanciar `RuntimeParams` manualmente (ex.: variaveis de ambiente).
2. Instanciar `JobExecutionConfig`.
3. Chamar `ExecutionResolver.resolve(params, job_config)` para obter `ExecutionContext`.
4. (Opcional) Validar datas com `validate_runtime_dates(date_start, date_end)` — o resolver ja chama isso internamente.
5. Aplicar filtro no DataFrame: `df_filtered = apply_runtime_filter(df, context, job_config.date_column)`.
6. Obter o config de escrita efetivo: `write_config = effective_write_config(base_config, context)`.
7. Chamar `manager.write(df_filtered, write_config)`.

### Excecoes de runtime

| Excecao | Quando e lancada |
|---|---|
| **RuntimeParamsError** | Erro generico de parametros de runtime (base para as demais) |
| **InvalidRuntimeParamsError** | `date_end` sem `date_start`, ou `date_start` > `date_end` |
| **UnsupportedExecutionModeError** | O job nao suporta o modo resolvido (ex.: full_refresh=True mas `supports_full_refresh=False`) |

Todas herdam de `LibSparkError`; podem ser capturadas de forma especifica ou junto com outras excecoes da lib.

### Exemplo: job simples com params da DAG (spark-submit --data_inicio=...)

```python
from lib_spark import (
    GlueTableManager, WriteConfig, WriteMode, LoadStrategy,
    ExecutionMode, ExecutionResolver, parse_runtime_params_from_argv,
    RuntimeParams, JobExecutionConfig,
    apply_runtime_filter, effective_write_config,
)

manager = GlueTableManager(spark)

# Parametros vindos da DAG: spark-submit ... meu_job.py --data_inicio=2025-05-01 [--data_fim=...] [--full_refresh=true]
params = parse_runtime_params_from_argv()

job_config = JobExecutionConfig(
    default_mode=ExecutionMode.INCREMENTAL_DEFAULT,
    date_column="dt",
    supports_full_refresh=True,
    supports_from_date=True,
    supports_date_range=True,
)

context = ExecutionResolver.resolve(params, job_config)
df = spark.sql("SELECT * FROM staging.vendas")
df_filtered = apply_runtime_filter(df, context, job_config.date_column)
config = effective_write_config(
    WriteConfig(
        target_table="glue_catalog.silver.fato_vendas",
        write_mode=WriteMode.APPEND,
        load_strategy=LoadStrategy.INCREMENTAL,
        incremental_column="dt",
        incremental_value="2024-01-01",
    ),
    context,
)
result = manager.write(df_filtered, config)
```

### Exemplo: full_refresh + datas (trigger manual)

Se o usuario passar `full_refresh=true` e `date_start=2024-02-01`, o modo resolvido sera **FULL_REFRESH** (full_refresh tem prioridade). O `effective_write_config` trocara a estrategia para FULL e o `apply_runtime_filter` nao aplicara filtro de data. Se passar apenas `date_start=2024-02-01`, o modo sera **FROM_DATE** e o filtro sera `dt >= "2024-02-01"`.

### Exemplo: fluxo completo (resolver + filtro + write)

```python
from lib_spark import (
    ExecutionMode, RuntimeParams, JobExecutionConfig,
    ExecutionResolver, apply_runtime_filter, effective_write_config,
    GlueTableManager, WriteConfig, WriteMode, LoadStrategy,
)

params = RuntimeParams(full_refresh=False, date_start="2024-01-01", date_end="2024-01-31")
job_config = JobExecutionConfig(
    default_mode=ExecutionMode.INCREMENTAL_DEFAULT,
    date_column="updated_at",
    supports_full_refresh=True,
    supports_from_date=True,
    supports_date_range=True,
)
context = ExecutionResolver.resolve(params, job_config)
# context.execution_mode == ExecutionMode.DATE_RANGE
# context.effective_date_start == "2024-01-01", context.effective_date_end == "2024-01-31"

df = manager.read("glue_catalog.bronze.eventos")
df_filtered = apply_runtime_filter(df, context, job_config.date_column)
config = effective_write_config(
    WriteConfig(
        target_table="glue_catalog.silver.eventos",
        write_mode=WriteMode.APPEND,
        load_strategy=LoadStrategy.INCREMENTAL,
        incremental_column="updated_at",
        incremental_value="2023-12-01",
    ),
    context,
)
result = manager.write(df_filtered, config)
```

---

## Otimizacao de escrita

Um dos problemas mais comuns em pipelines PySpark e a criacao excessiva de small files (arquivos muito pequenos no storage). Isso degrada a performance de leitura, aumenta custos de metadados e torna consultas lentas.

A `lib-spark` resolve isso de forma automatica. **Sem nenhuma configuracao, a biblioteca ja aplica defaults sensiveis que reduzem small files.**

### Como funciona

Antes de cada escrita, a biblioteca:

1. **Define o tamanho alvo de arquivo** via propriedade Iceberg `write.target-file-size-bytes` (default: 512MB)
2. **Define o modo de distribuicao** via propriedade Iceberg `write.distribution-mode` (default: `hash`)
3. **Ajusta o advisory partition size** do Spark para guiar o tamanho das particoes (default: 1024MB)
4. **Reparticiona o DataFrame** pelas colunas de particao quando o numero de particoes do DataFrame excede o threshold minimo (default: 10)

### Uso sem configuracao (recomendado)

```python
result = manager.write(df, config)
```

Nenhum parametro extra necessario. A otimizacao e aplicada automaticamente com defaults sensiveis.

### Customizacao quando necessario

```python
from lib_spark import WriteOptimizationConfig

result = manager.write(
    df, config,
    optimization=WriteOptimizationConfig(
        target_file_size_mb=256,
        distribution_mode="range",
    ),
)
```

### Ordenacao fisica (sort order / clustering)

O campo `sort_columns` e o equivalente Iceberg do `CLUSTER BY` do BigQuery. Ao informar colunas de ordenacao, a biblioteca executa `ALTER TABLE ... WRITE ORDERED BY` antes da escrita, fazendo com que o Iceberg ordene os dados fisicamente dentro de cada data file.

**Por que usar?**

- Melhora drasticamente a performance de leitura para queries que filtram pelas colunas de sort, pois o Iceberg utiliza **data skipping** baseado em estatisticas min/max de cada arquivo.
- Reduz a quantidade de dados lidos em full scans parciais.
- Diferente do particionamento (que cria diretórios separados), o sort order organiza dados **dentro** dos arquivos.

**Comparacao com BigQuery:**

| BigQuery | Iceberg (`lib-spark`) |
|---|---|
| `CLUSTER BY region, event_date` | `sort_columns=["region", "event_date"]` |
| Reordenacao automatica em background | Ordenacao aplicada a cada escrita |
| Limite de 4 colunas | Sem limite de colunas |

```python
from lib_spark import WriteOptimizationConfig

result = manager.write(
    df, config,
    optimization=WriteOptimizationConfig(
        sort_columns=["region", "event_date"],
    ),
)
```

Se `sort_columns` estiver vazio (default), nenhum sort order e configurado e o comportamento atual e preservado.

### Desativacao explicita

```python
result = manager.write(
    df, config,
    optimization=WriteOptimizationConfig(enabled=False),
)
```

---

## Manutencao de tabelas

Alem da otimizacao de escrita, tabelas Iceberg acumulam snapshots e podem conter small files historicos de escritas anteriores. A `lib-spark` oferece rotinas de manutencao opcionais que podem ser executadas automaticamente apos cada escrita.

**Manutencao e desativada por padrao.** Ative-a explicitamente quando necessario.

### Diferenca entre otimizacao e manutencao

| Aspecto | Otimizacao de escrita | Manutencao |
|---|---|---|
| Quando atua | **Durante** a escrita | **Depois** da escrita |
| Objetivo | Prevenir criacao de small files novos | Limpar historico, compactar arquivos antigos |
| Default | Ativada automaticamente | Desativada por padrao |
| Exemplos | Reparticionamento, tamanho de arquivo alvo | expire_snapshots, rewrite_data_files |

### Expirar snapshots

Remove snapshots antigos, reduzindo metadados e custos de storage de historico. **Nao resolve small files.**

```python
from lib_spark import MaintenanceConfig

result = manager.write(
    df, config,
    maintenance=MaintenanceConfig(
        enabled=True,
        snapshot_retention_days=15,
        retain_last_snapshots=3,
    ),
)
```

### Compactar data files

Resolve small files historicos reescrevendo arquivos pequenos em arquivos maiores.

```python
result = manager.write(
    df, config,
    maintenance=MaintenanceConfig(
        enabled=True,
        rewrite_data_files=True,
        rewrite_data_files_min_input_files=20,
    ),
)
```

### Manutencao completa

```python
result = manager.write(
    df, config,
    maintenance=MaintenanceConfig(
        enabled=True,
        expire_snapshots=True,
        snapshot_retention_days=7,
        retain_last_snapshots=3,
        rewrite_data_files=True,
        rewrite_manifests=True,
    ),
)

for action in result.maintenance_actions:
    print(f"Manutencao executada: {action}")
```

---

## Politicas de schema

A politica de schema controla o que acontece quando o schema do DataFrame diverge do schema da tabela destino. Configurada via `schema_policy` no `WriteConfig`.

### STRICT

**Padrao.** Qualquer divergencia gera falha imediata. Nenhuma alteracao e aplicada.

```python
config = WriteConfig(
    target_table="glue_catalog.db.tabela",
    write_mode=WriteMode.APPEND,
    schema_policy=SchemaPolicy.STRICT,
)
```

**Comportamento:**
- Coluna nova no DataFrame -> **falha**
- Coluna removida do DataFrame -> **falha**
- Tipo diferente -> **falha**

**Quando usar:** Ambientes de producao onde o schema deve ser rigidamente controlado.

---

### SAFE_SCHEMA_EVOLUTION

Permite que a tabela destino evolua de forma segura e automatica, sem intervencao manual, desde que as mudancas detectadas sejam compativeis. Qualquer alteracao destrutiva ou arriscada e bloqueada automaticamente, protegendo a integridade dos dados.

Na pratica, isso significa que:

- **Colunas novas** presentes no DataFrame mas ausentes na tabela sao adicionadas automaticamente via `ALTER TABLE ADD COLUMNS`.
- **Tipos compativeis** sao promovidos automaticamente (ex.: `int` para `long`), sem perda de dados.
- **Colunas ausentes no DataFrame** mas existentes na tabela sao preservadas -- a biblioteca nunca remove colunas da tabela destino.
- **Mudancas inseguras** (narrowing de tipo, conversoes incompativeis) geram falha imediata com mensagem clara indicando quais colunas e tipos causaram o bloqueio.

```python
config = WriteConfig(
    target_table="glue_catalog.db.tabela",
    write_mode=WriteMode.APPEND,
    schema_policy=SchemaPolicy.SAFE_SCHEMA_EVOLUTION,
)
```

**Tabela de comportamento detalhado:**

| Situacao | Acao executada |
|---|---|
| Coluna nova no DataFrame | `ALTER TABLE ADD COLUMNS` -- adicionada automaticamente |
| Tipo `int` -> `long` | `ALTER TABLE ALTER COLUMN TYPE` -- promovido automaticamente |
| Tipo `float` -> `double` | Promovido automaticamente |
| Decimal precision widening | Promovido automaticamente |
| Coluna ausente no DataFrame | Ignorada -- coluna permanece na tabela, log de aviso emitido |
| Tipo `string` -> `int` | **Falha** -- mudanca insegura bloqueada |
| Tipo `long` -> `int` | **Falha** -- narrowing bloqueado |

**Promocoes de tipo reconhecidas como seguras:**
- `byte` -> `short`, `int`, `long`
- `short` -> `int`, `long`
- `int` -> `long`
- `float` -> `double`
- `decimal(p1,s1)` -> `decimal(p2,s2)` onde `p2 >= p1` e `s2 >= s1`

**Operacoes bloqueadas por padrao (sempre, independentemente da politica):**
- Remocao automatica de colunas
- Rename automatico de colunas
- Narrowing de tipos (long -> int, double -> float)
- Mudancas incompativeis (string -> int, int -> boolean)
- Alteracoes de particionamento

---

### FAIL_ON_DIFF

Detecta a divergencia, loga o diff e falha sem aplicar nenhuma alteracao.

```python
config = WriteConfig(
    target_table="glue_catalog.db.tabela",
    write_mode=WriteMode.APPEND,
    schema_policy=SchemaPolicy.FAIL_ON_DIFF,
)
```

**Quando usar:** Quando voce quer ser avisado sobre diferencas de schema sem que nenhuma evolucao automatica ocorra.

---

### CUSTOM

Reservado para regras customizadas. Na v1, comporta-se como `SAFE_SCHEMA_EVOLUTION`. O campo `custom_schema_rules` no `WriteConfig` esta disponivel para extensoes futuras.

---

## Validacoes automaticas

Antes de cada escrita, a biblioteca executa as seguintes validacoes:

| Validacao                 | Descricao                                                              | Excecao                    |
| ---------------------------| ------------------------------------------------------------------------| ----------------------------|
| Parametros do config      | target_table nao vazio, formato correto, campos obrigatorios presentes | `InvalidConfigError`       |
| Existencia da tabela      | Tabela deve existir no catalogo                                        | `TableNotFoundError`       |
| Chaves de merge           | Todas as merge_keys devem existir no DataFrame e na tabela             | `MergeKeyError`            |
| Colunas de particao       | partition_columns devem corresponder as particoes reais da tabela      | `PartitionValidationError` |
| Compatibilidade de schema | Tipos de colunas comuns devem ser compativeis                          | `SchemaValidationError`    |
| DataFrame nao vazio       | DataFrame deve conter pelo menos um registro                           | `InvalidConfigError`       |

Todas as validacoes ocorrem **antes** da escrita. Se qualquer validacao falhar, a operacao e abortada sem efeitos colaterais.

---

## Auditoria e observabilidade

Toda operacao de escrita e automaticamente auditada via Python `logging`.

### Loggers

| Logger | Escopo |
|---|---|
| `lib_spark` | Logger raiz da biblioteca |
| `lib_spark.audit` | Inicio, sucesso e falha de cada operacao |
| `lib_spark.writer` | Detalhes de cada escrita |
| `lib_spark.validator` | Validacoes executadas |
| `lib_spark.catalog` | Resolucao de metadados |
| `lib_spark.schema_manager` | Comparacao e evolucao de schema |
| `lib_spark.optimization` | Otimizacao de escrita (reparticionamento, propriedades) |
| `lib_spark.maintenance` | Manutencao pos-escrita (snapshots, compactacao) |
| `lib_spark.reader` | Leituras de tabela |

### Formato dos logs de auditoria

```
AUDIT START   | table=glue_catalog.db.tabela | mode=append | strategy=full
AUDIT SUCCESS | table=glue_catalog.db.tabela | mode=append | records=1500 | optimized=True | duration=12.34s
AUDIT SUCCESS | table=glue_catalog.db.tabela | mode=merge  | records=500  | optimized=True | duration=8.50s | maintenance=[expire_snapshots(...)]
AUDIT FAILURE | table=glue_catalog.db.tabela | mode=merge  | error=... | duration=2.10s
```

### Configurando o logging no seu job

```python
import logging

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(name)s] %(levelname)s - %(message)s",
)
```

Para logs mais detalhados da biblioteca:

```python
logging.getLogger("lib_spark").setLevel(logging.DEBUG)
```

---

## Tratamento de erros

Todas as excecoes da biblioteca herdam de `LibSparkError`, permitindo captura generalizada ou especifica.

### Hierarquia de excecoes

```
LibSparkError
  ├── InvalidConfigError
  ├── TableNotFoundError
  ├── MergeKeyError
  ├── PartitionValidationError
  ├── SchemaValidationError
  ├── SchemaEvolutionBlockedError
  ├── RuntimeParamsError
  ├── InvalidRuntimeParamsError
  └── UnsupportedExecutionModeError
```

### Captura generalizada

```python
from lib_spark import LibSparkError

try:
    result = manager.write(df, config)
except LibSparkError as e:
    print(f"Erro na biblioteca: {e}")
```

### Captura especifica

```python
from lib_spark import (
    TableNotFoundError,
    MergeKeyError,
    SchemaEvolutionBlockedError,
    InvalidConfigError,
)

try:
    result = manager.write(df, config)
except TableNotFoundError:
    print("Tabela nao encontrada no catalogo.")
except MergeKeyError as e:
    print(f"Problema com chaves de merge: {e}")
except SchemaEvolutionBlockedError as e:
    print(f"Evolucao de schema bloqueada: {e}")
except InvalidConfigError as e:
    print(f"Configuracao invalida: {e}")
```

### WriteResult em caso de falha

Mesmo quando ocorre erro, o `WriteResult` e populado internamente com informacoes de auditoria antes da excecao ser levantada. Se voce precisar capturar o resultado em caso de falha:

```python
result = None
try:
    result = manager.write(df, config)
except LibSparkError:
    pass
# result sera None - o erro e propagado via excecao
```

---

## Arquitetura interna

```
GlueTableManager (core.py)
   │
   ├── CatalogResolver (catalog/resolver.py)
   │      Resolve nome da tabela, verifica existencia,
   │      le schema e particoes do catalogo
   │
   ├── Validators (validator/validators.py)
   │      Valida config, chaves, particoes,
   │      compatibilidade de schema, DataFrame vazio,
   │      WriteOptimizationConfig, MaintenanceConfig
   │
   ├── SchemaManager
   │   ├── comparator.py   →  compare_schemas() → SchemaDiff
   │   └── evolver.py      →  apply_evolution() → ALTER TABLE
   │
   ├── WriteOptimizer (optimization/write_optimizer.py)
   │      Define propriedades Iceberg, advisory size,
   │      reparticiona DataFrame antes da escrita
   │
   ├── ExecutionPlanner (planner/execution_planner.py)
   │      Resolve writer, aplica filtro incremental,
   │      monta ExecutionPlan
   │
   ├── Writers (writer/)
   │   ├── AppendWriter          →  writeTo().append()
   │   ├── OverwriteTableWriter  →  INSERT OVERWRITE TABLE
   │   ├── OverwritePartitionsWriter → writeTo().overwritePartitions()
   │   └── MergeWriter           →  MERGE INTO ... SQL
   │
   ├── TableMaintenance (maintenance/table_maintenance.py)
   │      expire_snapshots, rewrite_data_files,
   │      rewrite_manifests (pos-escrita)
   │
   ├── TableReader (reader/table_reader.py)
   │      Leitura full ou incremental
   │
   └── AuditLogger (audit/logger.py)
          Registra inicio, sucesso, falha, duracao,
          otimizacao aplicada, acoes de manutencao
```

### Fluxo de execucao do `write()`

```
 1. validate_config(config)
 2. validate_optimization_config(optimization)
 3. validate_maintenance_config(maintenance)
 4. catalog.resolve(target_table)  →  TableMetadata
 5. validate_table_exists(metadata)
 6. validate_merge_keys(config, df.schema, table.schema)       [se MERGE]
 7. validate_partition_columns(config, metadata)                [se OVERWRITE_PARTITIONS]
 8. compare_schemas(df.schema, table.schema)  →  SchemaDiff
 9. apply_evolution(spark, table, diff, policy)                 [se diff encontrado]
10. validate_schema_compatibility(df.schema, table.schema)      [se sem diff]
11. planner.prepare_dataframe(df, config)                       [filtro incremental]
12. validate_dataframe_not_empty(prepared_df)
13. optimizer.apply(spark, df, table, optimization)             [otimizacao]
14. writer.execute(spark, prepared_df, table_name)
15. maintenance.run(spark, table_name, maintenance)             [se habilitado]
16. audit.finish_success(result, records)  →  WriteResult
```

---

## Referencia de configuracao

### WriteMode (enum)

| Valor | Descricao |
|---|---|
| `WriteMode.APPEND` | Adiciona novos registros |
| `WriteMode.OVERWRITE_TABLE` | Substitui toda a tabela |
| `WriteMode.OVERWRITE_PARTITIONS` | Substitui particoes impactadas |
| `WriteMode.MERGE` | Upsert por chave (insert + update) |

### LoadStrategy (enum)

| Valor | Descricao |
|---|---|
| `LoadStrategy.FULL` | Processa todos os dados |
| `LoadStrategy.INCREMENTAL` | Filtra por coluna de controle |

### SchemaPolicy (enum)

| Valor | Descricao |
|---|---|
| `SchemaPolicy.STRICT` | Falha em qualquer divergencia |
| `SchemaPolicy.SAFE_SCHEMA_EVOLUTION` | Evolucao segura: adiciona colunas novas, promove tipos compativeis, bloqueia mudancas destrutivas |
| `SchemaPolicy.FAIL_ON_DIFF` | Detecta diff e falha sem aplicar |
| `SchemaPolicy.CUSTOM` | Regras customizadas (v1: igual a SAFE_SCHEMA_EVOLUTION) |

---

## Exemplos completos

### 1. Carga full com append

```python
from lib_spark import GlueTableManager, WriteConfig, WriteMode

manager = GlueTableManager(spark)

df = spark.sql("SELECT * FROM staging.novos_pedidos")

result = manager.write(df, WriteConfig(
    target_table="glue_catalog.vendas.fato_pedidos",
    write_mode=WriteMode.APPEND,
))

print(f"Escritos: {result.records_written} registros em {result.duration_seconds:.1f}s")
```

### 2. Merge incremental com evolucao de schema

```python
from lib_spark import (
    GlueTableManager, WriteConfig, WriteMode,
    LoadStrategy, SchemaPolicy,
)

manager = GlueTableManager(spark)

df = spark.sql("SELECT * FROM raw.clientes_cdc")

result = manager.write(df, WriteConfig(
    target_table="glue_catalog.silver.dim_cliente",
    write_mode=WriteMode.MERGE,
    load_strategy=LoadStrategy.INCREMENTAL,
    merge_keys=["id_cliente"],
    incremental_column="data_atualizacao",
    incremental_value="2024-06-01",
    schema_policy=SchemaPolicy.SAFE_SCHEMA_EVOLUTION,
))

if result.schema_changes:
    for col in result.schema_changes.added_columns:
        print(f"Coluna adicionada automaticamente: {col.name} ({col.dataType})")

print(f"Merge concluido: {result.records_written} registros")
```

### 3. Reprocessamento de particoes

```python
from lib_spark import GlueTableManager, WriteConfig, WriteMode

manager = GlueTableManager(spark)

df_correcao = spark.sql("""
    SELECT * FROM staging.vendas_corrigidas
    WHERE ano = 2024 AND mes = 6
""")

result = manager.write(df_correcao, WriteConfig(
    target_table="glue_catalog.vendas.fato_vendas",
    write_mode=WriteMode.OVERWRITE_PARTITIONS,
    partition_columns=["ano", "mes"],
))

print(f"Particoes reescritas: {result.records_written} registros")
```

### 4. Overwrite completo de dimensao

```python
from lib_spark import GlueTableManager, WriteConfig, WriteMode

manager = GlueTableManager(spark)

df_dim = spark.sql("SELECT * FROM staging.dim_produto_completa")

result = manager.write(df_dim, WriteConfig(
    target_table="glue_catalog.silver.dim_produto",
    write_mode=WriteMode.OVERWRITE_TABLE,
))

print(f"Tabela recriada com {result.records_written} registros")
```

### 5. Leitura + escrita no mesmo pipeline

```python
from lib_spark import GlueTableManager, WriteConfig, WriteMode, LoadStrategy

manager = GlueTableManager(spark)

# Ler incrementalmente da camada bronze
df_bronze = manager.read(
    "glue_catalog.bronze.transacoes",
    incremental_column="event_time",
    incremental_value="2024-06-01T00:00:00",
)

# Transformar
df_silver = df_bronze.select("id", "valor", "event_time", "tipo")

# Escrever na silver via merge
result = manager.write(df_silver, WriteConfig(
    target_table="glue_catalog.silver.transacoes",
    write_mode=WriteMode.MERGE,
    merge_keys=["id"],
))

print(f"Pipeline concluido: {result.records_written} registros em {result.duration_seconds:.1f}s")
```

### 6. Tratamento de erros em producao

```python
import logging
from lib_spark import (
    GlueTableManager, WriteConfig, WriteMode,
    LibSparkError, TableNotFoundError, SchemaEvolutionBlockedError,
)

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("meu_job")

manager = GlueTableManager(spark)

try:
    result = manager.write(df, WriteConfig(
        target_table="glue_catalog.silver.fato_vendas",
        write_mode=WriteMode.MERGE,
        merge_keys=["id_venda"],
    ))
    logger.info("Escrita concluida com sucesso: %d registros", result.records_written)

except TableNotFoundError:
    logger.error("Tabela destino nao encontrada. Verifique o Glue Catalog.")
    raise

except SchemaEvolutionBlockedError as e:
    logger.error("Mudanca de schema bloqueada: %s", e)
    raise

except LibSparkError as e:
    logger.error("Erro na lib_spark: %s", e)
    raise
```

### 7. Escrita otimizada com manutencao completa

```python
from lib_spark import (
    GlueTableManager, WriteConfig, WriteMode,
    WriteOptimizationConfig, MaintenanceConfig,
    SchemaPolicy,
)

manager = GlueTableManager(spark)

result = manager.write(
    df=df,
    config=WriteConfig(
        target_table="glue_catalog.silver.fato_vendas",
        write_mode=WriteMode.MERGE,
        merge_keys=["id_venda"],
        schema_policy=SchemaPolicy.SAFE_SCHEMA_EVOLUTION,
    ),
    optimization=WriteOptimizationConfig(
        target_file_size_mb=256,
        distribution_mode="hash",
    ),
    maintenance=MaintenanceConfig(
        enabled=True,
        expire_snapshots=True,
        snapshot_retention_days=15,
        retain_last_snapshots=3,
        rewrite_data_files=True,
    ),
)

print(f"Registros: {result.records_written}")
print(f"Otimizacao aplicada: {result.optimization_applied}")
print(f"Manutencao: {result.maintenance_actions}")
```

### 8. Escrita com sort order (clustering)

```python
from lib_spark import (
    GlueTableManager, WriteConfig, WriteMode,
    WriteOptimizationConfig,
)

manager = GlueTableManager(spark)

result = manager.write(
    df=df,
    config=WriteConfig(
        target_table="glue_catalog.silver.fato_eventos",
        write_mode=WriteMode.APPEND,
    ),
    optimization=WriteOptimizationConfig(
        sort_columns=["region", "event_date"],
    ),
)
# Dados ordenados fisicamente por region e event_date dentro de cada data file
# Queries com filtro WHERE region = 'BR' serao significativamente mais rapidas
```

### 9. Uso minimo (defaults fazem tudo)

```python
from lib_spark import GlueTableManager, WriteConfig, WriteMode

manager = GlueTableManager(spark)

result = manager.write(
    df,
    WriteConfig(
        target_table="glue_catalog.silver.fato_vendas",
        write_mode=WriteMode.APPEND,
    ),
)
# Otimizacao de escrita aplicada automaticamente
# Manutencao nao executada (default desativado)
```

---

## Build e distribuicao

### Gerar o pacote .whl

```bash
pip install build
python -m build
```

O arquivo `.whl` sera gerado em `dist/lib_spark-0.1.0-py3-none-any.whl`.

### Deploy no EMR

**Opcao 1: via `--py-files`**
```bash
aws s3 cp dist/lib_spark-0.1.0-py3-none-any.whl s3://meu-bucket/libs/

spark-submit \
    --py-files s3://meu-bucket/libs/lib_spark-0.1.0-py3-none-any.whl \
    meu_job.py
```

**Opcao 2: via bootstrap action (instala no cluster)**
```bash
#!/bin/bash
sudo pip install s3://meu-bucket/libs/lib_spark-0.1.0-py3-none-any.whl
```

**Opcao 3: via EMR Step**
```bash
aws emr add-steps --cluster-id j-XXXXX --steps \
    Type=CUSTOM_JAR,Name="Spark Job",Jar="command-runner.jar",\
    Args=["spark-submit","--py-files","s3://bucket/libs/lib_spark-0.1.0-py3-none-any.whl","s3://bucket/jobs/meu_job.py"]
```

---

## Testes

### Executar testes localmente

```bash
pip install -e ".[dev]"
pytest
```

### Estrutura de testes

| Arquivo                          | Escopo                                                                                 |
| ----------------------------------| ----------------------------------------------------------------------------------------|
| `tests/conftest.py`              | SparkSession local com catalogo Iceberg hadoop                                         |
| `tests/test_config.py`           | Enums, dataclasses, SchemaDiff                                                         |
| `tests/test_validators.py`       | Todas as funcoes de validacao                                                          |
| `tests/test_schema_manager.py`   | Comparacao de schemas e promocoes de tipo                                              |
| `tests/test_writers.py`          | Append, overwrite, merge com tabelas Iceberg reais                                     |
| `tests/test_planner.py`          | Resolucao de writer e filtro incremental                                               |
| `tests/test_runtime.py`          | ExecutionResolver, apply_runtime_filter, effective_write_config, validacoes de runtime |
| `tests/test_core_integration.py` | Fluxo completo via GlueTableManager (inclui testes de optimization e maintenance)      |

Os testes de integracao criam tabelas Iceberg em um catalogo hadoop local (sem necessidade de AWS).

### Testes no EMR (script `scripts/test_on_emr.py`)

O script `scripts/test_on_emr.py` executa uma suíte de testes contra tabelas Iceberg reais no Glue Catalog (para uso como EMR step). E obrigatorio informar o **bucket S3** de uma das formas:

- **Variavel de ambiente:** `export LIB_SPARK_TEST_BUCKET=meu-bucket`
- **Argumento:** `spark-submit ... test_on_emr.py meu-bucket`

Ver `INFRASTRUCTURE.md` para o provisionamento completo da infraestrutura AWS e o comando do EMR step.

---

## Nota de migracao (v0.1.0)

Na versao `0.1.0`, a politica de schema anteriormente chamada `EVOLVE_SAFE` foi renomeada para `SAFE_SCHEMA_EVOLUTION`.

Se voce possui jobs que referenciam o nome anterior, atualize conforme abaixo:

```python
# Antes
schema_policy=SchemaPolicy.EVOLVE_SAFE

# Depois
schema_policy=SchemaPolicy.SAFE_SCHEMA_EVOLUTION
```

O comportamento da politica permanece identico -- apenas o nome do enum foi alterado para melhor clareza. O valor serializado tambem mudou de `"evolve_safe"` para `"safe_schema_evolution"`. Se voce persiste esse valor em arquivos de configuracao, atualize-os tambem.
