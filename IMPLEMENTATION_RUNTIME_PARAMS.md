# Guia de implementação: parâmetros de runtime em DAGs geradas (Airflow + Lambda)

Este documento descreve **como** foi implementado o suporte a parâmetros de trigger manual (`full_refresh`, `date_start`, `date_end`) em um framework que **gera** DAGs Airflow dinamicamente (via Lambda) e repassa esses valores aos jobs Spark. Serve como referência para replicar a abordagem em outro projeto ou para outro agente continuar a evolução.

---

## 1. Contexto do problema

- **O que existe:** Uma Lambda lê configuração (ex.: YAML no S3), monta um template de DAG Airflow e gera arquivos `.py` que são enviados ao bucket do MWAA. As DAGs orquestram steps Spark no EMR.
- **O que se quer:** Na interface de **trigger manual** do Airflow, o usuário poder informar parâmetros (ex.: full refresh, intervalo de datas) e esses valores devem ser **validados**, **normalizados** e **repassados** ao `spark-submit` de cada job.
- **Restrições:** A Lambda gera o código da DAG em tempo de deploy; os valores dos parâmetros só existem em **runtime** (quando o usuário dispara a DAG). Por isso os argumentos do step não podem ser fixos no código gerado — precisam ser resolvidos na hora em que cada task roda.

---

## 2. Arquitetura da solução (visão geral)

```
[Trigger manual no Airflow]
    ↓
  params: { full_refresh, date_start, date_end }  (defaults na DAG)
    ↓
  Task 1: validate_params (PythonOperator)
    - Lê context["params"]
    - Valida regras (ex.: date_end exige date_start; date_start <= date_end)
    - Normaliza (ex.: full_refresh → "true"/"false")
    - Faz XCom.push(key="full_refresh", ...), idem date_start, date_end
    ↓
  Task 2: get_cluster_id (como já existia)
    ↓
  Tasks 3..N: EmrAddStepsOperator (um por job)
    - steps[].HadoopJarStep.Args = [ ..., "--full-refresh", "{{ ti.xcom_pull(...) }}", ... ]
    - Em runtime, o Airflow renderiza o Jinja e o step recebe os valores vindos do XCom
```

Pontos importantes:

1. **Params na DAG:** declarados em `DAG(params={...})`. Aparecem na UI de trigger e têm defaults; se ninguém preencher, a execução segue normal.
2. **Task de validação única:** Uma única task (`validate_params`) lê os params, valida e grava no XCom. Todas as tasks de step Spark consomem esse XCom via Jinja.
3. **Jinja no código gerado:** O arquivo `.py` gerado pela Lambda contém **strings** com sintaxe Jinja (ex.: `{{ ti.xcom_pull(task_ids='validate_params', key='full_refresh') | default('false') }}`). Essas strings são **renderizadas pelo Airflow** quando a task do step roda (não quando a DAG é parseada). Por isso o operador que monta o step (ex.: `EmrAddStepsOperator`) precisa ter o campo `steps` (e, se aplicável, `Args` dentro dele) como **template field**.
4. **Ordem do grafo:** `validate_params` deve rodar **antes** de qualquer step que use os params. No nosso caso: `validate_params >> get_cluster_id >> [jobs...]`.

---

## 3. Onde cada coisa vive

| O quê | Onde | Motivo |
|-------|------|--------|
| Definição dos params (nome, tipo, default) | **Template da DAG** (arquivo que a Lambda usa para gerar o `.py`) | A DAG gerada precisa expor os mesmos params para toda execução; não vem do YAML por job. |
| Lógica de validação e normalização | **Template da DAG** (função Python embutida no template + PythonOperator) | A validação roda no Airflow, com acesso a `context["params"]` e XCom. |
| Injeção dos argumentos no spark-submit | **Lambda (gerador)** | A Lambda monta a lista de Args do step e inclui nessa lista as **strings Jinja** que referenciam o XCom da task de validação. |
| Nome fixo da task de validação | **Lambda (constante)** | A Lambda precisa escrever Jinja com o mesmo `task_ids='validate_params'` que está no template; usar constante evita typo. |

---

## 4. Implementação passo a passo

### 4.1 Template da DAG (ex.: `lib/spark_dag_template.py`)

O template é uma string Python (com placeholders `$var`) que vira o código-fonte da DAG. Alterações:

#### 4.1.1 Declarar os params no construtor da DAG

Incluir no `with DAG(...)` um bloco `params=` com os defaults:

```python
with DAG(
    dag_id="$dag_id",
    ...
    params={
        "full_refresh": False,
        "date_start": "",
        "date_end": "",
    },
    default_args={...},
    ...
) as dag:
```

- `full_refresh`: boolean no Python; na UI do Airflow pode vir como bool ou string.
- `date_start` / `date_end`: string; vazios quando não informados.
- Assim, **quem não informar nada** mantém execução normal (defaults).

#### 4.1.2 Função de validação e normalização

Adicionar uma função que será usada por um `PythonOperator`:

```python
def validate_and_normalize_runtime_params(**context):
    '''
    Valida e normaliza parâmetros de runtime (trigger manual).
    Regras: date_end exige date_start; date_start <= date_end.
    Repassa full_refresh, date_start, date_end ao XCom para os jobs Spark.
    '''
    ti = context["ti"]
    params = context.get("params") or {}

    # full_refresh: aceitar bool ou string ("true", "1", "yes")
    full_refresh = params.get("full_refresh", False)
    if isinstance(full_refresh, str):
        full_refresh = full_refresh.strip().lower() in ("true", "1", "yes")
    full_refresh = bool(full_refresh)

    date_start = str(params.get("date_start") or "").strip()
    date_end = str(params.get("date_end") or "").strip()

    # Regras de validação
    if date_end and not date_start:
        raise ValueError(
            "Parâmetro date_end exige date_start. Informe date_start quando usar date_end."
        )
    if date_start and date_end and date_start > date_end:
        raise ValueError(
            f"date_start ({date_start}) deve ser menor ou igual a date_end ({date_end})."
        )

    # Sempre gravar no XCom no mesmo formato (string) para o job consumir
    ti.xcom_push(key="full_refresh", value="true" if full_refresh else "false")
    ti.xcom_push(key="date_start", value=date_start)
    ti.xcom_push(key="date_end", value=date_end)
```

- Uso de `context.get("params") or {}`: evita erro se por algum motivo não houver params.
- Comparação `date_start > date_end`: funciona para strings em formato ISO (YYYY-MM-DD). Se o projeto usar outro formato, a comparação pode ser ajustada (ou converter para date).
- XCom sempre com strings: facilita injetar no `spark-submit` como argumentos únicos (ex.: `--full-refresh true`).

#### 4.1.3 Task que chama a função de validação

Dentro do `with DAG(...) as dag:`:

```python
validate_params = PythonOperator(
    task_id="validate_params",
    python_callable=validate_and_normalize_runtime_params,
)
```

O `task_id` deve ser **fixo** e igual ao que a Lambda usará no Jinja (ex.: `validate_params`).

#### 4.1.4 Ordem no grafo

A primeira dependência do grafo deve ser a validação antes do restante (ex.: antes de obter cluster e rodar os jobs):

```python
validate_params >> get_cluster_id
$graph_dependencies
```

`$graph_dependencies` é substituído pela Lambda e contém as linhas do tipo `get_cluster_id >> job1`, `job1 >> sensor1`, etc. Assim, o fluxo fica: `validate_params >> get_cluster_id >> job1 >> ...`.

---

### 4.2 Lambda / gerador de DAG (ex.: `lib/create_dag.py`)

A Lambda monta a lista `Args` de cada step (spark-submit) e grava isso no código Python da DAG. Os valores dos params só existem em runtime, então a Lambda não pode colocar valores literais — ela coloca **strings que são expressões Jinja**, para o Airflow renderizar quando a task do step rodar.

#### 4.2.1 Constante com o task_id da validação

Para não repetir o nome da task em vários pontos e evitar typo no Jinja:

```python
VALIDATE_PARAMS_TASK_ID = "validate_params"
```

Use essa constante em todo lugar que gerar Jinja referenciando essa task.

#### 4.2.2 Função que retorna os “argumentos de runtime” como Jinja

Cada step deve receber sempre os mesmos argumentos, na mesma ordem (ex.: `--full-refresh`, valor, `--date-start`, valor, `--date-end`, valor). A Lambda gera uma lista de strings: nomes dos argumentos e, no lugar dos valores, strings que são templates Jinja:

```python
def _runtime_params_jinja_args():
    """
    Retorna a lista de argumentos de runtime (full_refresh, date_start, date_end)
    como strings Jinja para serem renderizadas em runtime pelo Airflow.
    """
    return [
        "--full-refresh",
        "{{ ti.xcom_pull(task_ids='" + VALIDATE_PARAMS_TASK_ID + "', key='full_refresh') | default('false') }}",
        "--date-start",
        "{{ ti.xcom_pull(task_ids='" + VALIDATE_PARAMS_TASK_ID + "', key='date_start') | default('') }}",
        "--date-end",
        "{{ ti.xcom_pull(task_ids='" + VALIDATE_PARAMS_TASK_ID + "', key='date_end') | default('') }}",
    ]
```

Observações:

- Dentro do Jinja usamos **aspas simples** para `task_ids` e `key`, para que o JSON (ou o literal Python) que envolve essa string não precise escapar aspas duplas.
- `| default('false')` e `| default('')` garantem que, se o XCom não existir (caso extremo), ainda há um valor previsível.
- O resultado são 6 itens: 3 nomes de argumentos + 3 “valores” (templated). O job Spark sempre recebe os três parâmetros; quando vazios, as strings vazias são passadas mesmo assim.

#### 4.2.3 Montar a lista completa de Args do step e serializar

Onde hoje você monta a lista de argumentos do `spark-submit` (ex.: `_build_spark_submit_args`), mantenha essa função retornando a lista **estática** (entrypoint, --jars, --py-files, application_args, etc.). Depois **concatene** a lista de argumentos de runtime (Jinja):

```python
# Exemplo dentro da função que gera o código do EmrAddStepsOperator
base_step_args = _build_spark_submit_args({**job_record, "job_id": job_base_name})
full_step_args = base_step_args + _runtime_params_jinja_args()
hadoop_step_args = json.dumps(full_step_args, ensure_ascii=False)
```

- `base_step_args`: lista de strings (ex.: `["spark-submit", "--deploy-mode", "cluster", ..., "--app_name", "meu_job"]`).
- `_runtime_params_jinja_args()`: lista de 6 strings, sendo 3 com conteúdo Jinja.
- `json.dumps(..., ensure_ascii=False)`: produz uma string que representa uma lista Python/JSON. No arquivo gerado isso vira algo como:
  `"Args": ["spark-submit", ..., "--full-refresh", "{{ ti.xcom_pull(task_ids='validate_params', key='full_refresh') | default('false') }}", ...]`

Quando o Airflow carrega o `.py`, `Args` é uma lista de strings. Algumas dessas strings contêm `{{ ... }}`. Quando a task do step roda, o Airflow (se o operador tiver `steps` como template field) re-renderiza esses campos e substitui o Jinja pelos valores do XCom, e o EMR recebe os argumentos já preenchidos.

#### 4.2.4 Garantir que o operador usa template no campo certo

No Airflow 2.x, `EmrAddStepsOperator` costuma ter `template_fields` incluindo `steps` (e por vezes `job_flow_id`). Isso é suficiente: ao rodar a task, o Airflow aplica Jinja nos valores de `steps`. Se o seu operador for outro, confira na documentação ou no código-fonte se o campo que contém a lista de steps (e dentro dela os `Args`) é templated. Sem isso, as strings `{{ ... }}` seriam enviadas literalmente ao EMR.

---

## 5. Detalhes e armadilhas

### 5.1 Escapamento no JSON/Python gerado

- A Lambda gera código Python (um arquivo `.py`). A lista `Args` é escrita como literal Python (ou como string JSON que será parte de um literal). As strings Jinja contêm aspas duplas e chaves. Ao usar `json.dumps(lista, ensure_ascii=False)`, as aspas duplas dentro das strings da lista são escapadas com `\"`. No resultado final no `.py` você terá algo como:
  `"Args": ["spark-submit", ..., \"--full-refresh\", \"{{ ti.xcom_pull(task_ids='validate_params', key='full_refresh') | default('false') }}\", ...]`
  Isso é válido em Python. O importante é **não** quebrar a sintaxe Jinja (por exemplo, usando aspas duplas dentro do Jinja e gerando escape incorreto). Por isso usamos aspas simples no Jinja para `task_ids` e `key`.

### 5.2 Duplo escape de chaves no f-string (Python)

No mesmo arquivo da Lambda, ao gerar o código do step, pode ser que você use f-strings e que o template da DAG use `{{` e `}}` para placeholders. No nosso caso, o template da DAG usa `$var` para substituição (string.Template), e o Jinja que vai para o arquivo gerado usa `{{ }}`. Na Lambda, ao montar a string que contém o Jinja, não use f-string com `{{` se quiser que `{{` apareça literalmente no arquivo. Aqui, o Jinja é montado como string normal (ex.: `"{{ ti.xcom_pull(...) }}"`) e concatenado; depois essa string entra na lista e passa por `json.dumps`, então as chaves chegam corretamente no arquivo gerado.

### 5.3 Execução agendada vs. trigger manual

Em execução agendada, o Airflow não passa params adicionais; os usados são os **defaults** definidos em `DAG(params={...})`. A task `validate_params` roda do mesmo jeito, lê esses defaults, valida (que não falha com valores vazios) e preenche o XCom. Os steps recebem então `--full-refresh false --date-start  --date-end `. Ou seja, não é necessário tratar execução agendada de forma especial.

### 5.4 Compatibilidade com jobs que não usam os params

Jobs que não leem `--full-refresh`, `--date-start` ou `--date-end` continuam funcionando: esses argumentos extras são apenas ignorados pelo script. Manter sempre os três argumentos (mesmo vazios) torna o contrato previsível para quem quiser usar no futuro.

---

## 6. Checklist para replicar em outro projeto

- [ ] **Template da DAG**
  - [ ] Adicionar `params={...}` no `DAG(...)` com os nomes e defaults desejados.
  - [ ] Implementar função de validação/normalização que lê `context["params"]`, aplica regras e faz `ti.xcom_push` para cada chave.
  - [ ] Criar `PythonOperator` com essa função e `task_id` fixo (ex.: `validate_params`).
  - [ ] Incluir no grafo a aresta `validate_params >> [primeira_task_existente]`.
- [ ] **Gerador (Lambda ou script que gera o .py)**
  - [ ] Definir constante com o `task_id` da task de validação.
  - [ ] Criar função que retorna a lista de “argumentos de runtime” (nomes + strings Jinja que fazem `ti.xcom_pull` para cada key).
  - [ ] Na montagem dos Args do step (spark-submit ou equivalente), concatenar: `args_estaticos + args_runtime_jinja`.
  - [ ] Serializar a lista para o formato esperado no código gerado (ex.: `json.dumps`) sem quebrar o Jinja.
- [ ] **Operador do Airflow**
  - [ ] Confirmar que o campo que contém os Args do step (ex.: `steps` em `EmrAddStepsOperator`) está em `template_fields` para que o Jinja seja renderizado em runtime.
- [ ] **Documentação**
  - [ ] Documentar os params (nome, tipo, default), as regras de validação e o formato dos argumentos recebidos pelo job (ex.: sempre `--full-refresh`, `--date-start`, `--date-end`).
  - [ ] Deixar explícito que a DAG só valida e repassa; a lógica de negócio (o que fazer com full_refresh ou com o intervalo de datas) fica no job.

---

## 7. Resumo dos arquivos e trechos alterados (referência)

| Arquivo | O que foi feito |
|---------|------------------|
| **Template (ex.: `spark_dag_template.py`)** | (1) `params={...}` no `DAG(...)`. (2) Função `validate_and_normalize_runtime_params(**context)` com validação e `xcom_push`. (3) Task `validate_params = PythonOperator(..., python_callable=validate_and_normalize_runtime_params)`. (4) No corpo do `with DAG(...)`: linha `validate_params >> get_cluster_id` antes de `$graph_dependencies`. |
| **Lambda/create_dag (ex.: `create_dag.py`)** | (1) `VALIDATE_PARAMS_TASK_ID = "validate_params"`. (2) Função `_runtime_params_jinja_args()` retornando lista de 6 strings (nomes + Jinja). (3) Onde se monta o step: `full_step_args = base_step_args + _runtime_params_jinja_args()` e usar `full_step_args` na serialização (ex.: `json.dumps`) que gera o bloco `Args` no código da DAG. |

Com isso, outro agente ou outro projeto consegue reproduzir a mesma ideia: params na DAG, uma task de validação que escreve no XCom, e argumentos de step montados com Jinja que leem esse XCom em runtime.
