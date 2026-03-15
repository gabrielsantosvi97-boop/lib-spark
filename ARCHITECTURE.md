# Arquitetura e Guia de Manutencao -- lib-spark

Este documento descreve como a biblioteca foi construida, quais decisoes de design foram tomadas e como dar manutencao ou estender cada componente. Destinado a desenvolvedores que precisam modificar, depurar ou evoluir a `lib-spark`.

---

## Indice

1. [Visao geral da arquitetura](#visao-geral-da-arquitetura)
2. [Estrutura de diretorios](#estrutura-de-diretorios)
3. [Fluxo de execucao detalhado](#fluxo-de-execucao-detalhado)
4. [Modulos internos](#modulos-internos)
   - [config.py -- Modelos de dados](#configpy----modelos-de-dados)
   - [exceptions.py -- Hierarquia de erros](#exceptionspy----hierarquia-de-erros)
   - [core.py -- Orquestrador (GlueTableManager)](#corepy----orquestrador-gluetablemanager)
   - [catalog/resolver.py -- Resolucao de metadados](#catalogresolverpy----resolucao-de-metadados)
   - [validator/validators.py -- Validacoes](#validatorvalidatorspy----validacoes)
   - [schema_manager/ -- Comparacao e evolucao de schema](#schema_manager----comparacao-e-evolucao-de-schema)
   - [planner/execution_planner.py -- Resolucao de writer e filtro](#plannerexecution_plannerpy----resolucao-de-writer-e-filtro)
   - [optimization/write_optimizer.py -- Otimizacao de escrita](#optimizationwrite_optimizerpy----otimizacao-de-escrita)
   - [writer/ -- Estrategias de escrita](#writer----estrategias-de-escrita)
   - [maintenance/table_maintenance.py -- Manutencao pos-escrita](#maintenancetable_maintenancepy----manutencao-pos-escrita)
   - [reader/table_reader.py -- Leitura de tabelas](#readertable_readerpy----leitura-de-tabelas)
   - [audit/logger.py -- Auditoria](#auditloggerpy----auditoria)
5. [Decisoes de design](#decisoes-de-design)
6. [Padroes e convencoes](#padroes-e-convencoes)
7. [Como estender a biblioteca](#como-estender-a-biblioteca)
   - [Adicionar um novo WriteMode](#adicionar-um-novo-writemode)
   - [Adicionar uma nova SchemaPolicy](#adicionar-uma-nova-schemapolicy)
   - [Adicionar um novo campo de configuracao](#adicionar-um-novo-campo-de-configuracao)
   - [Adicionar uma nova rotina de manutencao](#adicionar-uma-nova-rotina-de-manutencao)
8. [Estrategia de testes](#estrategia-de-testes)
9. [Build e empacotamento](#build-e-empacotamento)
10. [Dependencias e ambiente](#dependencias-e-ambiente)
11. [Troubleshooting comum](#troubleshooting-comum)

---

## Visao geral da arquitetura

A biblioteca segue o padrao **Facade + Strategy**:

- **Facade:** `GlueTableManager` e o unico ponto de entrada publico. O consumidor interage apenas com essa classe.
- **Strategy:** Cada modo de escrita (`append`, `overwrite_table`, `overwrite_partitions`, `merge`) e implementado por uma classe concreta que herda de `BaseWriter`. O planner resolve qual writer usar em runtime.

O fluxo e linear e determinista: validar -> comparar schema -> preparar dados -> otimizar -> escrever -> manter -> auditar. Nenhum passo depende de estado global ou de efeitos colaterais de outro passo.

```
                          ┌─────────────────────┐
                          │   GlueTableManager   │  ← Facade (core.py)
                          │       .write()       │
                          └──────────┬──────────┘
                                     │
        ┌────────────────────────────┼────────────────────────────┐
        │                            │                            │
   ┌────▼─────┐              ┌───────▼──────┐             ┌──────▼──────┐
   │ Validate │              │   Resolve    │             │   Compare   │
   │  config  │              │   catalog    │             │   schemas   │
   │validators│              │  metadata    │             │ comparator  │
   └────┬─────┘              │  resolver    │             │  evolver    │
        │                    └───────┬──────┘             └──────┬──────┘
        │                            │                           │
        └────────────────────────────┼───────────────────────────┘
                                     │
                          ┌──────────▼──────────┐
                          │  Prepare DataFrame  │
                          │  (incremental filt) │
                          │  execution_planner  │
                          └──────────┬──────────┘
                                     │
                          ┌──────────▼──────────┐
                          │  Optimize write     │
                          │  write_optimizer    │
                          └──────────┬──────────┘
                                     │
                          ┌──────────▼──────────┐
                          │  Execute write      │
                          │  AppendWriter /     │
                          │  MergeWriter / ...  │
                          └──────────┬──────────┘
                                     │
                          ┌──────────▼──────────┐
                          │  Run maintenance    │
                          │  table_maintenance  │
                          └──────────┬──────────┘
                                     │
                          ┌──────────▼──────────┐
                          │  Audit & return     │
                          │  WriteResult        │
                          └─────────────────────┘
```

---

## Estrutura de diretorios

```
lib-spark/
├── src/
│   └── lib_spark/
│       ├── __init__.py              # API publica: exports de classes, enums, excecoes
│       ├── config.py                # Enums, dataclasses (WriteConfig, WriteOptimizationConfig, etc.)
│       ├── core.py                  # GlueTableManager -- orquestrador principal
│       ├── exceptions.py            # Hierarquia de excecoes customizadas
│       ├── audit/
│       │   └── logger.py            # AuditLogger -- registro de inicio/sucesso/falha
│       ├── catalog/
│       │   └── resolver.py          # CatalogResolver -- resolve metadados via Spark SQL
│       ├── maintenance/
│       │   └── table_maintenance.py # TableMaintenance -- expire_snapshots, rewrite_data_files
│       ├── optimization/
│       │   └── write_optimizer.py   # WriteOptimizer -- propriedades Iceberg, sort order, repartition
│       ├── planner/
│       │   └── execution_planner.py # ExecutionPlanner -- resolve writer, aplica filtro incremental
│       ├── reader/
│       │   └── table_reader.py      # TableReader -- leitura full e incremental
│       ├── schema_manager/
│       │   ├── comparator.py        # compare_schemas() -- detecta diff entre schemas
│       │   └── evolver.py           # apply_evolution() -- aplica ALTER TABLE conforme politica
│       ├── validator/
│       │   └── validators.py        # Funcoes de validacao (config, merge keys, particoes, schema)
│       └── writer/
│           ├── base.py              # BaseWriter (ABC)
│           ├── append.py            # AppendWriter
│           ├── overwrite.py         # OverwriteTableWriter, OverwritePartitionsWriter
│           └── merge.py             # MergeWriter
├── tests/
│   ├── conftest.py                  # Fixtures: SparkSession local, banco e tabelas Iceberg
│   ├── test_config.py               # Testes de dataclasses e enums
│   ├── test_validators.py           # Testes de todas as funcoes de validacao
│   ├── test_schema_manager.py       # Testes de comparacao de schema e promocoes de tipo
│   ├── test_writers.py              # Testes de cada writer contra tabelas Iceberg reais
│   ├── test_planner.py              # Testes de resolucao de writer e filtro incremental
│   └── test_core_integration.py     # Testes end-to-end via GlueTableManager
├── pyproject.toml                   # Configuracao de build, dependencias, pytest
└── README.md                        # Documentacao de uso para o consumidor
```

**Principio:** Cada subdiretorio contem um unico modulo com responsabilidade clara. Um arquivo `__init__.py` vazio em cada pacote mantem a estrutura flat dentro de cada modulo.

---

## Fluxo de execucao detalhado

Abaixo esta o fluxo completo do metodo `GlueTableManager.write()`, com referencia ao arquivo e funcao responsavel por cada passo:

| Passo | Responsavel | Arquivo | O que faz |
|-------|-------------|---------|-----------|
| 1 | `validate_config()` | `validator/validators.py` | Valida target_table, write_mode, merge_keys, partition_columns, incremental_column |
| 2 | `validate_optimization_config()` | `validator/validators.py` | Valida distribution_mode, tamanhos, sort_columns |
| 3 | `validate_maintenance_config()` | `validator/validators.py` | Valida retention_days, retain_last, min_input_files |
| 4 | `CatalogResolver.resolve()` | `catalog/resolver.py` | Parseia nome da tabela, verifica existencia, le schema/particoes/location |
| 5 | `validate_table_exists()` | `validator/validators.py` | Aborta se tabela nao existe |
| 6 | `validate_merge_keys()` | `validator/validators.py` | Verifica que merge_keys existem no DataFrame e na tabela (somente modo MERGE) |
| 7 | `validate_partition_columns()` | `validator/validators.py` | Verifica que partition_columns correspondem as particoes da tabela (somente OVERWRITE_PARTITIONS) |
| 8 | `compare_schemas()` | `schema_manager/comparator.py` | Compara schemas e retorna `SchemaDiff` com colunas adicionadas, removidas e tipos alterados |
| 9 | `apply_evolution()` | `schema_manager/evolver.py` | Se houver diff, aplica ALTER TABLE conforme a `SchemaPolicy` (ou bloqueia) |
| 10 | `validate_schema_compatibility()` | `validator/validators.py` | Se nao houver diff, valida compatibilidade basica (safety net) |
| 11 | `ExecutionPlanner.prepare_dataframe()` | `planner/execution_planner.py` | Aplica filtro incremental se `LoadStrategy.INCREMENTAL` |
| 12 | `validate_dataframe_not_empty()` | `validator/validators.py` | Garante que ha pelo menos 1 registro |
| 13 | `WriteOptimizer.apply()` | `optimization/write_optimizer.py` | Define propriedades Iceberg, sort order, advisory partition size, reparticionamento |
| 14 | `Writer.execute()` | `writer/*.py` | Executa a escrita conforme o modo (append, overwrite, merge) |
| 15 | `TableMaintenance.run()` | `maintenance/table_maintenance.py` | Executa manutencao pos-escrita se habilitada |
| 16 | `AuditLogger.finish_success()` | `audit/logger.py` | Registra log de sucesso e preenche o `WriteResult` final |

Se qualquer passo falhar, o fluxo entra no bloco `except`, chama `AuditLogger.finish_error()` e re-lanca a excecao.

---

## Modulos internos

### config.py -- Modelos de dados

Centraliza todos os modelos de dados como `dataclass` e `Enum`. Nenhum modelo contem logica de negocio -- sao DTOs puros.

**Enums:**
- `WriteMode` -- modos de escrita (APPEND, OVERWRITE_TABLE, OVERWRITE_PARTITIONS, MERGE)
- `LoadStrategy` -- estrategias de carga (FULL, INCREMENTAL)
- `SchemaPolicy` -- politicas de evolucao de schema (STRICT, SAFE_SCHEMA_EVOLUTION, FAIL_ON_DIFF, CUSTOM)

**Dataclasses principais:**
- `WriteConfig` -- parametros obrigatorios e opcionais de uma escrita
- `WriteOptimizationConfig` -- configuracao de otimizacao (defaults habilitados)
- `MaintenanceConfig` -- configuracao de manutencao (defaults desabilitados)
- `SchemaDiff` -- resultado da comparacao de schemas, com propriedades computadas `is_compatible` e `has_differences`
- `TypeChange` -- representa uma mudanca de tipo em uma coluna
- `TableMetadata` -- metadados resolvidos do catalogo (schema, particoes, location, existencia)
- `ExecutionPlan` -- agrupa writer + metadados + config (usado internamente pelo planner)
- `WriteResult` -- resultado retornado ao consumidor apos cada escrita

**Constantes:**
- `VALID_DISTRIBUTION_MODES` -- tupla com modos validos para o Iceberg (`"none"`, `"hash"`, `"range"`)

**Convencao:** Todo novo campo de configuracao deve ter um default sensato. Campos obrigatorios sao apenas `target_table` e `write_mode` no `WriteConfig`.

---

### exceptions.py -- Hierarquia de erros

Todas as excecoes herdam de `LibSparkError`:

```
LibSparkError
  ├── InvalidConfigError        # Configuracao invalida ou inconsistente
  ├── TableNotFoundError        # Tabela nao encontrada no catalogo
  ├── MergeKeyError             # Chave de merge ausente no DataFrame ou na tabela
  ├── PartitionValidationError  # Coluna de particao invalida
  ├── SchemaValidationError     # Schema incompativel
  └── SchemaEvolutionBlockedError # Evolucao de schema bloqueada pela politica
```

**Convencao:** Nunca lance `Exception` diretamente. Sempre use uma subclasse de `LibSparkError` para que o consumidor possa capturar erros da lib de forma granular ou generalizada.

**Para adicionar um novo erro:** Crie uma classe em `exceptions.py`, herde de `LibSparkError`, e exporte no `__init__.py`.

---

### core.py -- Orquestrador (GlueTableManager)

`GlueTableManager` e a facade que o consumidor usa. Ela:

1. Recebe uma `SparkSession` no construtor
2. Instancia todos os componentes internos (catalog, planner, reader, audit, optimizer, maintenance)
3. Expoe dois metodos publicos: `write()` e `read()`

**Defaults internos:** Duas constantes de modulo definem os defaults:
- `_DEFAULT_OPTIMIZATION = WriteOptimizationConfig()` -- optimization habilitada com defaults
- `_DEFAULT_MAINTENANCE = MaintenanceConfig()` -- maintenance desabilitada

Quando o consumidor passa `optimization=None`, a lib usa `_DEFAULT_OPTIMIZATION`. Quando passa `maintenance=None`, usa `_DEFAULT_MAINTENANCE`.

**Tratamento de erros:** O `write()` envolve todo o fluxo em `try/except`. Erros de `LibSparkError` sao re-lancados diretamente. Qualquer outro erro e encapsulado em `LibSparkError` com mensagem descritiva.

**Para modificar o fluxo de escrita:** Altere `core.py`. A ordem dos passos e deliberada -- validacoes primeiro, otimizacao antes da escrita, manutencao depois.

---

### catalog/resolver.py -- Resolucao de metadados

`CatalogResolver` e responsavel por:

1. **Parsear nomes de tabela:** Aceita `catalogo.database.tabela` ou `database.tabela`. Retorna um `TableMetadata` com as partes separadas.
2. **Verificar existencia:** Usa `spark.catalog.tableExists()` com fallback para `spark.table()`.
3. **Ler schema:** Usa `spark.table(name).schema` para obter o `StructType`.
4. **Ler colunas de particao:** Executa `DESCRIBE TABLE` e parseia a secao `# Partition` do output.
5. **Ler location:** Executa `DESCRIBE TABLE EXTENDED` e busca a linha `location`.

**Particularidade do Spark Catalog API:** O metodo `tableExists(tableName, dbName)` do `spark.catalog` inverte a ordem (tabela primeiro, banco depois). O resolver trata isso.

**Para adicionar novos metadados:** Crie um novo metodo `_get_*()` e chame-o dentro de `resolve()`. Adicione o campo ao `TableMetadata` em `config.py`.

---

### validator/validators.py -- Validacoes

Contem funcoes puras de validacao (sem efeitos colaterais). Cada funcao:
- Recebe um objeto de configuracao ou dados
- Lanca uma excecao especifica se a validacao falhar
- Retorna silenciosamente se tudo estiver ok

**Funcoes existentes:**
| Funcao | Valida | Excecao |
|--------|--------|---------|
| `validate_config()` | target_table, write_mode, merge_keys, partition_columns, incremental_column | `InvalidConfigError` |
| `validate_table_exists()` | existencia da tabela no catalogo | `TableNotFoundError` |
| `validate_merge_keys()` | chaves de merge existem no DataFrame e na tabela | `MergeKeyError` |
| `validate_partition_columns()` | partition_columns correspondem as particoes reais | `PartitionValidationError` |
| `validate_schema_compatibility()` | tipos de colunas comuns sao compativeis | `SchemaValidationError` |
| `validate_dataframe_not_empty()` | DataFrame tem pelo menos 1 registro | `InvalidConfigError` |
| `validate_optimization_config()` | distribution_mode valido, tamanhos positivos, sort_columns | `InvalidConfigError` |
| `validate_maintenance_config()` | retention_days > 0, retain_last >= 1 | `InvalidConfigError` |

**Convencao:** Validacoes que dependem de configs opcionais (optimization, maintenance) verificam `config.enabled` primeiro e retornam imediatamente se desabilitada.

**Para adicionar uma nova validacao:** Crie a funcao em `validators.py`, importe-a em `core.py` e chame-a no ponto adequado do fluxo.

---

### schema_manager/ -- Comparacao e evolucao de schema

Dividido em dois arquivos com responsabilidades distintas:

**`comparator.py` -- Deteccao de diferencas**

`compare_schemas(source_schema, target_schema)` compara campo a campo e retorna um `SchemaDiff`:
- Colunas no source que nao existem no target -> `added_columns`
- Colunas no target que nao existem no source -> `removed_columns`
- Colunas com tipo diferente -> `type_changes` (cada uma classificada como safe ou unsafe)

A classificacao de seguranca usa `SAFE_PROMOTIONS` (um set de tuplas de tipos) e `_is_safe_decimal_widening()`.

**Promocoes seguras reconhecidas:**
- `byte -> short, int, long`
- `short -> int, long`
- `int -> long`
- `float -> double`
- `decimal(p1,s1) -> decimal(p2,s2)` onde `p2 >= p1` e `s2 >= s1`

**Para adicionar um novo tipo de promocao segura:** Adicione a tupla `(tipo_origem, tipo_destino)` ao set `SAFE_PROMOTIONS` em `comparator.py`.

**`evolver.py` -- Aplicacao de evolucao**

`apply_evolution(spark, table_name, diff, policy)` decide o que fazer com base na politica:

| Politica | Comportamento |
|----------|---------------|
| `STRICT` | Qualquer diff -> bloqueia (`SchemaEvolutionBlockedError`) |
| `FAIL_ON_DIFF` | Qualquer diff -> loga e bloqueia (sem aplicar) |
| `SAFE_SCHEMA_EVOLUTION` | Adiciona colunas novas, promove tipos seguros, bloqueia unsafe |
| `CUSTOM` | Na v1 comporta-se como `SAFE_SCHEMA_EVOLUTION` |

A funcao `_safe_schema_evolution()` executa DDL real:
- `ALTER TABLE ... ADD COLUMNS (coluna tipo)` para colunas novas
- `ALTER TABLE ... ALTER COLUMN tipo TYPE novo_tipo` para promocoes seguras

**Para adicionar uma nova politica:** Adicione o enum em `config.py`, crie a funcao `_nova_politica()` em `evolver.py` e adicione o `if` em `apply_evolution()`.

---

### planner/execution_planner.py -- Resolucao de writer e filtro

`ExecutionPlanner` tem duas responsabilidades:

1. **`resolve_writer(write_mode)`** -- Usa o `_WRITER_REGISTRY` (dicionario `WriteMode -> WriterClass`) para instanciar o writer correto.
2. **`prepare_dataframe(df, config)`** -- Aplica filtro incremental (`col > value`) quando `LoadStrategy.INCREMENTAL`.

O `_WRITER_REGISTRY` e um dicionario de modulo:
```python
_WRITER_REGISTRY = {
    WriteMode.APPEND: AppendWriter,
    WriteMode.OVERWRITE_TABLE: OverwriteTableWriter,
    WriteMode.OVERWRITE_PARTITIONS: OverwritePartitionsWriter,
    WriteMode.MERGE: MergeWriter,
}
```

**Para registrar um novo writer:** Adicione a entrada ao `_WRITER_REGISTRY`.

---

### optimization/write_optimizer.py -- Otimizacao de escrita

`WriteOptimizer.apply()` executa quatro acoes em sequencia antes da escrita:

1. **`_set_table_write_properties()`** -- Define propriedades Iceberg na tabela via `ALTER TABLE SET TBLPROPERTIES`:
   - `write.target-file-size-bytes` -- tamanho alvo por arquivo
   - `write.distribution-mode` -- como o Iceberg distribui dados entre arquivos

2. **`_set_sort_order()`** -- Define sort order na tabela via `ALTER TABLE ... WRITE ORDERED BY (col1, col2)`. Executado somente quando `sort_columns` nao esta vazio. Equivalente ao `CLUSTER BY` do BigQuery.

3. **`_set_advisory_partition_size()`** -- Define `spark.sql.iceberg.advisory-partition-size` na SparkSession para guiar o tamanho das particoes.

4. **`_maybe_repartition()`** -- Reparticiona o DataFrame pelas colunas de particao da tabela quando:
   - `repartition_by_partition_columns` e `True`
   - `partition_columns` foram fornecidas
   - O numero atual de particoes do DataFrame excede `min_input_files_before_repartition`

**Tratamento de erros:** As operacoes de `ALTER TABLE` usam `try/except` com `logger.warning` em vez de falhar. Isso e intencional -- a otimizacao e best-effort para nao bloquear a escrita se a tabela nao suportar a operacao.

---

### writer/ -- Estrategias de escrita

Todas as estrategias implementam `BaseWriter` (ABC) com o metodo:
```python
def execute(self, spark, df, table_name, **kwargs) -> int
```

Retornam o numero de registros escritos.

| Writer | Mecanismo | Spark API usada |
|--------|-----------|-----------------|
| `AppendWriter` | Adiciona registros | `df.writeTo(table).append()` |
| `OverwriteTableWriter` | Substitui toda a tabela | `INSERT OVERWRITE TABLE ... SELECT * FROM view` |
| `OverwritePartitionsWriter` | Substitui particoes impactadas | `df.writeTo(table).overwritePartitions()` |
| `MergeWriter` | Upsert por chave | `MERGE INTO ... USING ... ON ... WHEN MATCHED ... WHEN NOT MATCHED ...` |

**MergeWriter:** Cria uma temp view do DataFrame, monta o SQL de `MERGE INTO` com a condicao de join baseada nas `merge_keys`, e executa via `spark.sql()`. A temp view e removida apos a execucao.

**OverwriteTableWriter:** Usa `INSERT OVERWRITE TABLE` via Spark SQL em vez de `df.writeTo().overwrite()` para garantir compatibilidade com Iceberg.

**Para adicionar um novo writer:** Crie a classe herdando de `BaseWriter` em um novo arquivo dentro de `writer/`, e registre-a no `_WRITER_REGISTRY` do planner.

---

### maintenance/table_maintenance.py -- Manutencao pos-escrita

`TableMaintenance.run()` executa rotinas Iceberg pos-escrita via procedures `CALL catalog.system.*`:

| Rotina | Comando | Quando executa |
|--------|---------|----------------|
| `expire_snapshots` | `CALL catalog.system.expire_snapshots(table, older_than, retain_last)` | `config.expire_snapshots == True` |
| `rewrite_data_files` | `CALL catalog.system.rewrite_data_files(table, options)` | `config.rewrite_data_files == True` |
| `rewrite_manifests` | `CALL catalog.system.rewrite_manifests(table)` | `config.rewrite_manifests == True` |

**Helpers importantes:**
- `_extract_catalog(table_name)` -- Extrai o prefixo do catalogo do nome qualificado (ex.: `glue_catalog` de `glue_catalog.db.tabela`). Se nao houver prefixo, usa `spark_catalog`.
- `_bare_table_name(table_name)` -- Remove o prefixo do catalogo, retornando `database.tabela`.

As procedures do Iceberg requerem o nome do catalogo separado (`CALL catalogo.system.procedure(table => 'db.tabela')`), por isso esses helpers existem.

**Tratamento de erros:** Cada rotina tem `try/except` com `logger.warning`. A falha em uma rotina nao impede a execucao das demais.

---

### reader/table_reader.py -- Leitura de tabelas

`TableReader` oferece:
- `read_full(table_name)` -- Le toda a tabela via `spark.table()`
- `read_incremental(table_name, col, value)` -- Le toda a tabela e aplica filtro `col > value`
- `read(table_name, ...)` -- Metodo unificado que decide entre full e incremental

**Nota:** A leitura incremental le a tabela inteira e filtra no Spark. O Iceberg com predicado pushdown otimiza isso automaticamente via data skipping.

---

### audit/logger.py -- Auditoria

`AuditLogger` gerencia o ciclo de vida de um `WriteResult`:

1. `start(config)` -- Cria um `WriteResult` com `success=False` e `started_at`. Loga `AUDIT START`.
2. `finish_success(result, records, ...)` -- Preenche sucesso, contagem, schema_changes, optimization, maintenance. Loga `AUDIT SUCCESS`.
3. `finish_error(result, error)` -- Preenche o erro e duracao. Loga `AUDIT FAILURE`.

**Formato dos logs:**
```
AUDIT START   | table=... | mode=... | strategy=...
AUDIT SUCCESS | table=... | mode=... | records=... | optimized=... | duration=...
AUDIT FAILURE | table=... | mode=... | error=... | duration=...
```

Todos os logs usam o logger `lib_spark.audit` com nivel INFO (sucesso) ou ERROR (falha).

---

## Decisoes de design

### 1. Facade unica em vez de multiplas classes

**Decisao:** O consumidor interage apenas com `GlueTableManager`.

**Motivo:** Reduz a superficie da API. O desenvolvedor nao precisa saber que existem writers, planners, optimizers ou validators internos. Ele chama `write()` e a lib cuida de tudo.

### 2. Configuracao via dataclasses em vez de dicionarios

**Decisao:** `WriteConfig`, `WriteOptimizationConfig` e `MaintenanceConfig` sao dataclasses com tipos explicitos e defaults.

**Motivo:** Autocompletar no IDE, validacao de tipos em linters, defaults documentados no proprio codigo, imutabilidade pratica.

### 3. Defaults sensiveis para otimizacao, manutencao desabilitada por padrao

**Decisao:** `WriteOptimizationConfig` e habilitada por default. `MaintenanceConfig` e desabilitada por default.

**Motivo:** Otimizacao de escrita (reducao de small files) beneficia todos os casos de uso sem efeitos colaterais. Manutencao (expire_snapshots, rewrite_data_files) altera o estado da tabela de forma mais invasiva, entao exige ativacao explicita.

### 4. Schema evolution como operacao DDL real

**Decisao:** A evolucao de schema executa `ALTER TABLE ADD COLUMNS` e `ALTER COLUMN TYPE` em vez de usar `mergeSchema` do Spark.

**Motivo:** `mergeSchema` e uma opcao write-time que pode ter comportamentos inesperados. ALTER TABLE e mais explicito, auditavel e controlavel por politica.

### 5. Writers stateless e descartaveis

**Decisao:** Cada writer e instanciado sob demanda pelo planner. Nao armazena estado.

**Motivo:** Simplifica testes, evita side effects entre escritas, e permite trocar a implementacao facilmente.

### 6. Otimizacao best-effort (nao falha a escrita)

**Decisao:** Se um `ALTER TABLE SET TBLPROPERTIES` falhar, a escrita continua.

**Motivo:** A otimizacao melhora performance mas nao e essencial para corretude. Falhar a escrita por causa de uma propriedade Iceberg nao suportada seria desproporcional.

### 7. Validacoes antes de qualquer efeito colateral

**Decisao:** Todas as validacoes executam antes da escrita e da evolucao de schema.

**Motivo:** Se algo esta errado, o consumidor descobre imediatamente sem ter alterado nenhum dado ou schema.

### 8. Excecoes tipadas em vez de codigos de erro

**Decisao:** Cada tipo de falha tem sua propria excecao (MergeKeyError, SchemaValidationError, etc.).

**Motivo:** O consumidor pode capturar erros de forma granular ou generalizada (`except LibSparkError`). As mensagens de erro incluem contexto (nomes de colunas, valores invalidos).

---

## Padroes e convencoes

### Logging

- Cada modulo cria seu logger: `logging.getLogger("lib_spark.<modulo>")`
- Loggers existentes: `lib_spark`, `lib_spark.audit`, `lib_spark.writer`, `lib_spark.validator`, `lib_spark.catalog`, `lib_spark.schema_manager`, `lib_spark.optimization`, `lib_spark.maintenance`, `lib_spark.reader`, `lib_spark.planner`
- Nivel INFO para operacoes normais, DEBUG para detalhes internos, WARNING para falhas nao-criticas, ERROR para falhas criticas

### Imports

- Todos os modulos usam `from __future__ import annotations` para permitir type hints com `|` em Python < 3.10
- Imports sao organizados: stdlib -> third-party (pyspark) -> lib_spark

### Type hints

- Todos os metodos publicos possuem type hints
- `List`, `Dict`, `Optional` sao importados de `typing` (compatibilidade com Python 3.9)

### Testes

- Testes unitarios para funcoes puras (validators, config, comparator)
- Testes de integracao com SparkSession real + catalogo Iceberg hadoop local
- Fixtures compartilhadas em `conftest.py`

---

## Como estender a biblioteca

### Adicionar um novo WriteMode

1. Adicione o valor ao enum `WriteMode` em `config.py`
2. Crie a classe em `writer/novo_writer.py` herdando de `BaseWriter`
3. Implemente `execute(self, spark, df, table_name, **kwargs) -> int`
4. Registre no `_WRITER_REGISTRY` em `planner/execution_planner.py`
5. Se precisar de validacoes especificas, adicione em `validators.py` e chame em `core.py`
6. Adicione testes em `test_writers.py` e `test_core_integration.py`
7. Exporte no `__init__.py` se necessario

### Adicionar uma nova SchemaPolicy

1. Adicione o valor ao enum `SchemaPolicy` em `config.py`
2. Crie a funcao `_nova_politica(spark, table_name, diff)` em `schema_manager/evolver.py`
3. Adicione o `if policy == SchemaPolicy.NOVA:` em `apply_evolution()`
4. Adicione testes em `test_schema_manager.py`

### Adicionar um novo campo de configuracao

1. Adicione o campo ao dataclass apropriado em `config.py` (com default)
2. Se precisar de validacao, adicione em `validators.py`
3. Integre no fluxo em `core.py` (ou no modulo que vai consumi-lo)
4. Atualize os testes de config e validators
5. Documente no `README.md`

### Adicionar uma nova rotina de manutencao

1. Adicione o campo booleano ao `MaintenanceConfig` em `config.py`
2. Crie o metodo `_nova_rotina()` em `maintenance/table_maintenance.py`
3. Adicione o `if config.nova_rotina:` no metodo `run()`
4. Adicione a validacao se necessario em `validators.py`
5. Atualize testes e documentacao

---

## Estrategia de testes

### Camadas de teste

| Camada | Arquivo | O que testa | SparkSession? |
|--------|---------|-------------|---------------|
| Config | `test_config.py` | Enums, defaults, dataclasses, SchemaDiff | Nao |
| Validacao | `test_validators.py` | Funcoes de validacao, mensagens de erro | Minimo (schema only) |
| Schema | `test_schema_manager.py` | Comparacao de schemas, promocoes de tipo | Nao |
| Writers | `test_writers.py` | Cada writer contra tabelas Iceberg reais | Sim (local) |
| Planner | `test_planner.py` | Resolucao de writer, filtro incremental | Sim (local) |
| Integracao | `test_core_integration.py` | Fluxo completo via GlueTableManager | Sim (local) |

### Ambiente de testes

O `conftest.py` configura uma SparkSession local com catalogo Iceberg hadoop:

```python
.config("spark.sql.catalog.local", "org.apache.iceberg.spark.SparkCatalog")
.config("spark.sql.catalog.local.type", "hadoop")
.config("spark.sql.catalog.local.warehouse", warehouse_dir)  # tmpdir
.config("spark.sql.defaultCatalog", "local")
```

Isso permite criar tabelas Iceberg reais sem AWS. O diretorio e temporario e limpo automaticamente.

### Fixtures

- `spark` (scope=session) -- SparkSession compartilhada entre todos os testes
- `test_db` (scope=function) -- Banco criado antes de cada teste e removido depois
- `sample_table` -- Tabela simples (id INT, name STRING, value DOUBLE)
- `partitioned_table` -- Tabela particionada por `year`

### Executando testes

```bash
pip install -e ".[dev]"
pytest
pytest -v                    # verbose
pytest -k "test_merge"       # filtrar por nome
pytest tests/test_config.py  # arquivo especifico
```

---

## Build e empacotamento

### pyproject.toml

A lib usa `setuptools` como build backend:

- **Nome do pacote:** `lib-spark`
- **Versao:** `0.1.0` (tambem em `__init__.py`)
- **Python minimo:** 3.9
- **Dependencias de runtime:** Nenhuma (PySpark e fornecido pelo EMR)
- **Dependencias de dev:** `pytest>=7.0`, `pyspark>=3.5.0`
- **Pacotes:** Buscados automaticamente em `src/`

### Gerando o .whl

```bash
pip install build
python -m build
# Gera dist/lib_spark-0.1.0-py3-none-any.whl
```

### Atualizando a versao

1. Altere `version` em `pyproject.toml`
2. Altere `__version__` em `src/lib_spark/__init__.py`

---

## Dependencias e ambiente

### Runtime (EMR)

| Dependencia | Fornecida por | Versao |
|-------------|---------------|--------|
| Python | EMR 7.10.0 | 3.9+ |
| PySpark | EMR 7.10.0 | 3.5.5 |
| Apache Iceberg | EMR 7.10.0 | 1.5.0 |
| AWS Glue Catalog | Configuracao do EMR | - |

A lib **nao declara** `pyspark` como dependencia de runtime para evitar conflito com a versao do EMR. O import de `pyspark` assume que ele ja esta disponivel.

### Desenvolvimento local

```bash
pip install -e ".[dev]"
```

Isso instala `pyspark>=3.5.0` e `pytest>=7.0` localmente. Para testes locais, o catalogo Iceberg hadoop e usado em vez do Glue.

---

## Troubleshooting comum

### "Table not found" em testes locais

O catalogo de teste usa o prefixo `local` (nao `glue_catalog`). Tabelas devem ser referenciadas como `local.test_db.tabela`.

### "Could not set table write properties"

Warning do optimizer. A tabela pode nao suportar as propriedades (ex.: tabela nao-Iceberg). A escrita continua normalmente.

### Testes lentos na primeira execucao

A primeira execucao baixa o JAR do Iceberg (~60MB) via `spark.jars.packages`. Execucoes subsequentes usam o cache do Maven.

### Conflito de SparkSession em testes

A fixture `spark` tem `scope="session"`, entao todos os testes compartilham a mesma instancia. Cada teste cria e destroi seu proprio database via `test_db`.

### "merge_keys are required for MergeWriter"

O `MergeWriter` valida internamente que `merge_keys` nao esta vazio. Essa validacao deveria ser capturada antes pelo `validate_config()`, mas existe como safety net.
