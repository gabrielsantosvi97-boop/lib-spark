# Estratégia para documentação externa via YAML na `lib-spark`

## Objetivo

Permitir que a documentação da tabela fique em um arquivo YAML externo ao código, e que essa documentação seja anexada automaticamente no momento da criação da tabela, além de poder ser sincronizada novamente depois.

A ideia é tratar o YAML como a **fonte oficial da documentação**, enquanto a biblioteca cuida da criação da tabela e da aplicação dessa documentação no catálogo.

---

## Princípio de design

A documentação **não deve ficar acoplada** à lógica de escrita.

Separar responsabilidades deixa a biblioteca mais limpa e facilita manutenção, evolução de schema e governança.

### Regra principal
- o writer cria a tabela
- o módulo de documentação aplica a documentação
- a sincronização da documentação deve poder ser executada separadamente

---

## Componentes internos sugeridos

### 1. `DocumentationResolver`
Responsável por:
- localizar o arquivo YAML
- carregar o conteúdo
- transformar o conteúdo em um objeto estruturado interno

### 2. `DocumentationValidator`
Responsável por:
- validar a estrutura do YAML
- validar coerência entre documentação e schema da tabela
- garantir que os campos obrigatórios existam
- validar limites de tamanho dos textos

### 3. `DocumentationApplier`
Responsável por:
- buscar os metadados atuais da tabela
- anexar descrição da tabela
- anexar comentários das colunas
- atualizar o catálogo com a nova documentação

---

## Fluxo recomendado na criação da tabela

### Fluxo de `create_table(...)`

1. receber a solicitação de criação da tabela
2. resolver o YAML de documentação
3. validar o YAML
4. criar a tabela Iceberg
5. buscar os metadados atuais da tabela no catálogo
6. aplicar:
   - descrição da tabela
   - comentários das colunas
7. atualizar o catálogo
8. registrar auditoria

---

## Fluxo recomendado para sincronização independente

### Fluxo de `sync_documentation(...)`

1. receber o nome da tabela
2. localizar o YAML correspondente
3. validar o conteúdo
4. carregar os metadados atuais da tabela
5. aplicar a documentação
6. atualizar o catálogo
7. registrar auditoria

Esse fluxo é útil quando:
- o YAML foi alterado
- houve schema evolution
- a documentação precisa ser corrigida sem recriar a tabela

---

## Métodos públicos sugeridos

### `create_table(...)`
Cria a tabela e, se houver documentação disponível, aplica a documentação automaticamente.

### `sync_documentation(...)`
Reaplica ou atualiza a documentação da tabela sem recriá-la.

---

## Estratégia de descoberta do YAML

A biblioteca pode suportar dois modos:

### 1. Convenção automática
A biblioteca encontra o YAML com base no nome da tabela.

Exemplo conceitual:
- tabela: `glue_catalog.silver.dim_cliente`
- arquivo: `docs/silver/dim_cliente.yml`

### 2. Caminho explícito
O usuário informa diretamente o caminho do arquivo YAML.

Isso mantém o uso simples, mas flexível.

---

## Estrutura conceitual do YAML

O YAML deve guardar principalmente **documentação**, e não duplicar desnecessariamente o schema técnico.

### Informações recomendadas
- nome da tabela
- descrição da tabela
- lista de colunas
  - nome
  - descrição
- metadados adicionais
  - owner
  - domínio
  - camada
  - tags opcionais

---

## Regras de validação recomendadas

Antes de aplicar a documentação, validar:

- a tabela documentada no YAML corresponde à tabela alvo
- toda coluna documentada existe no schema da tabela
- nomes das colunas batem exatamente
- campos obrigatórios de documentação estão preenchidos
- não existem colunas “sobrando” no YAML
- os tamanhos dos textos respeitam os limites do catálogo

---

## Comportamento padrão sugerido

### Se o YAML existir
- aplicar automaticamente a documentação após a criação da tabela

### Se o YAML não existir
- criar a tabela normalmente
- registrar aviso em log
- não falhar por padrão

---

## Modo estrito opcional

Adicionar uma configuração como:

- `documentation_required = True`

Nesse caso:
- se o YAML não existir, a criação falha

Isso é útil para tabelas mais governadas, como Silver e Gold.

---

## Integração com evolução de schema

Quando houver schema evolution, a documentação também precisa ser considerada.

### Fluxo recomendado
1. detectar mudança de schema
2. comparar schema atualizado com o YAML
3. decidir:
   - falhar
   - avisar
   - aplicar parcialmente
4. reaplicar a documentação para as colunas válidas

---

## Organização sugerida no projeto

```text
documentation/
├── models.py
├── resolver.py
├── validator.py
└── applier.py
```

---

## Modelos internos sugeridos

### `TableDocumentation`
Representa a documentação da tabela.

Campos conceituais:
- `table_name`
- `table_description`
- `columns`
- `owner`
- `domain`
- `layer`
- `tags`

### `ColumnDocumentation`
Representa a documentação de cada coluna.

Campos conceituais:
- `name`
- `description`

---

## Integração com o `GlueTableManager`

### Sugestão de responsabilidades

- `create_table(...)`
  - cria a tabela
  - chama a sincronização da documentação, se aplicável

- `sync_documentation(...)`
  - resolve o YAML
  - valida
  - aplica a documentação

---

## Diretriz principal de implementação

A biblioteca **não deve ler o YAML diretamente dentro do writer**.

O fluxo correto é:

- o writer cria a tabela
- o módulo de documentação sincroniza os metadados depois

Isso evita acoplamento indevido entre:
- escrita de dados
- criação de tabela
- governança documental

---

## Ordem recomendada de implementação

### Fase 1
- criar parser do YAML
- criar modelos internos de documentação

### Fase 2
- criar validador
- validar estrutura e coerência com schema

### Fase 3
- criar aplicador da documentação no catálogo

### Fase 4
- integrar no fluxo de criação da tabela

### Fase 5
- adicionar sincronização pós schema evolution

---

## Resumo final

A abordagem recomendada é:

- manter a documentação em YAML externo
- usar esse YAML como fonte oficial da documentação
- criar a tabela normalmente
- aplicar a documentação logo após a criação
- permitir reaplicação posterior via sincronização dedicada
- manter documentação, escrita e schema evolution como responsabilidades separadas

Essa estrutura deixa a `lib-spark` mais limpa, mais governável e mais fácil de evoluir.
