# Infraestrutura AWS -- lib-spark

Documento com todos os comandos utilizados para criar e destruir os recursos AWS necessarios para testar a `lib-spark` em ambiente real.

> **Substitua os placeholders** pelos valores da sua propria conta AWS antes de executar os comandos:
> - `ACCOUNT_ID` — ID da sua conta AWS (12 digitos)
> - `MY_BUCKET` — Nome do bucket S3 para artefatos e warehouse (ex.: `meu-bucket-lib-spark`)
> - `MY_IAM_USER` — Nome do usuario IAM que vai criar o cluster EMR
> - `SUBNET_ID` — ID da subnet onde o EMR subira (ex.: `subnet-xxxxxxxx`)
> - `VPC_ID` — ID da VPC (para tags da policy v2)
> - `REGION` — Regiao AWS (ex.: `sa-east-1`)

**Conta:** `ACCOUNT_ID`
**Regiao:** `REGION`
**Bucket S3:** `MY_BUCKET`
**Glue Database:** `lib_spark_test`

---

## Indice

1. [Pre-requisitos](#pre-requisitos)
2. [Passo 0 -- Permissoes IAM](#passo-0----permissoes-iam)
3. [Passo 1 -- Glue Database](#passo-1----glue-database)
4. [Passo 2 -- Build e upload dos artefatos](#passo-2----build-e-upload-dos-artefatos)
5. [Passo 3 -- IAM Roles do EMR](#passo-3----iam-roles-do-emr)
6. [Passo 4 -- Criar cluster EMR e executar testes](#passo-4----criar-cluster-emr-e-executar-testes)
7. [Passo 5 -- Monitorar execucao](#passo-5----monitorar-execucao)
8. [Destruicao dos recursos](#destruicao-dos-recursos)
9. [Estimativa de custos](#estimativa-de-custos)

---

## Pre-requisitos

- AWS CLI v2 instalada e configurada (`aws configure`)
- Python 3.9+ com `pip install build`
- Acesso a conta AWS na regiao desejada (`REGION`)

---

## Passo 0 -- Permissoes IAM

O usuario IAM que criara o cluster (`MY_IAM_USER`) precisa de permissoes adicionais. Execute os comandos abaixo como **root** ou como um usuario com **IAMFullAccess** no Console AWS.

### Opcao A: Via Console AWS (mais simples)

1. Acesse o IAM Console: https://console.aws.amazon.com/iam/
2. Clique em **Users** > **MY_IAM_USER**
3. Clique em **Add permissions** > **Attach policies directly**
4. Anexe as seguintes policies gerenciadas:
   - `AmazonEMRFullAccessPolicy_v2`
   - `AmazonS3FullAccess` (ou policy mais restrita abaixo)
   - `AWSGlueConsoleFullAccess`
   - `IAMFullAccess` (temporario, para criar os default roles do EMR)

### Opcao B: Via CLI (como root/admin)

```bash
# Anexar policies ao usuario (substitua MY_IAM_USER)
aws iam attach-user-policy --user-name MY_IAM_USER \
    --policy-arn arn:aws:iam::aws:policy/AmazonEMRFullAccessPolicy_v2

aws iam attach-user-policy --user-name MY_IAM_USER \
    --policy-arn arn:aws:iam::aws:policy/AmazonS3FullAccess

aws iam attach-user-policy --user-name MY_IAM_USER \
    --policy-arn arn:aws:iam::aws:policy/AWSGlueConsoleFullAccess

aws iam attach-user-policy --user-name MY_IAM_USER \
    --policy-arn arn:aws:iam::aws:policy/IAMFullAccess
```

### Opcao C: Policy customizada minima (recomendada para seguranca)

Crie uma inline policy para `MY_IAM_USER` com as permissoes minimas:

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Sid": "EMRAccess",
      "Effect": "Allow",
      "Action": [
        "elasticmapreduce:RunJobFlow",
        "elasticmapreduce:ListClusters",
        "elasticmapreduce:DescribeCluster",
        "elasticmapreduce:DescribeStep",
        "elasticmapreduce:ListSteps",
        "elasticmapreduce:TerminateJobFlows",
        "elasticmapreduce:AddJobFlowSteps"
      ],
      "Resource": "*"
    },
    {
      "Sid": "IAMPassRole",
      "Effect": "Allow",
      "Action": [
        "iam:PassRole",
        "iam:GetRole",
        "iam:CreateRole",
        "iam:AttachRolePolicy",
        "iam:PutRolePolicy",
        "iam:CreateInstanceProfile",
        "iam:AddRoleToInstanceProfile",
        "iam:ListRoles"
      ],
      "Resource": "*"
    },
    {
      "Sid": "EC2ForEMR",
      "Effect": "Allow",
      "Action": [
        "ec2:DescribeSubnets",
        "ec2:DescribeVpcs",
        "ec2:DescribeSecurityGroups",
        "ec2:DescribeInstances",
        "ec2:DescribeInstanceTypes",
        "ec2:CreateSecurityGroup",
        "ec2:AuthorizeSecurityGroupIngress",
        "ec2:AuthorizeSecurityGroupEgress",
        "ec2:RunInstances",
        "ec2:TerminateInstances",
        "ec2:CreateTags"
      ],
      "Resource": "*"
    },
    {
      "Sid": "GlueAccess",
      "Effect": "Allow",
      "Action": [
        "glue:*"
      ],
      "Resource": "*"
    },
    {
      "Sid": "S3Access",
      "Effect": "Allow",
      "Action": [
        "s3:*"
      ],
      "Resource": [
        "arn:aws:s3:::MY_BUCKET",
        "arn:aws:s3:::MY_BUCKET/*"
      ]
    }
  ]
}
```

**Apos anexar as permissoes, continue com os passos abaixo.**

---

## Passo 1 -- Glue Database

> **Status: JA CRIADO** - Este passo ja foi executado com sucesso.

```bash
# Criar database no Glue Catalog
aws glue create-database \
    --database-input '{
        "Name": "lib_spark_test",
        "Description": "Database de teste para lib-spark Iceberg",
        "LocationUri": "s3://MY_BUCKET/lib-spark/warehouse/"
    }' \
    --region REGION

# Verificar
aws glue get-database --name lib_spark_test --region REGION
```

---

## Passo 2 -- Build e upload dos artefatos

> **Status: JA EXECUTADO** - Os artefatos ja estao no S3.

```bash
# Build do pacote .whl
cd lib-spark
python -m build --wheel

# Upload do .whl para S3
aws s3 cp dist/lib_spark-0.1.0-py3-none-any.whl \
    s3://MY_BUCKET/lib-spark/artifacts/ \
    --region REGION

# Upload do script de teste
aws s3 cp scripts/test_on_emr.py \
    s3://MY_BUCKET/lib-spark/scripts/ \
    --region REGION

# Verificar uploads
aws s3 ls s3://MY_BUCKET/lib-spark/ \
    --recursive --region REGION
```

Artefatos no S3:
```
lib-spark/artifacts/lib_spark-0.1.0-py3-none-any.whl   (64 KB)
lib-spark/scripts/test_on_emr.py                        (10 KB)
```

---

## Passo 3 -- IAM Roles do EMR

> **Requer permissoes IAM (Passo 0)**

### 3.1 Criar default roles

O EMR precisa de roles padrao. Execute o comando abaixo para cria-los automaticamente:

```bash
aws emr create-default-roles --region REGION
```

Isso cria:
- `EMR_DefaultRole` -- role de servico do EMR (v1)
- `EMR_EC2_DefaultRole` -- instance profile para os nodos EC2
- `EMR_AutoScaling_DefaultRole` -- role de auto-scaling

### 3.2 Criar EMR_DefaultRole_V2

A policy `AmazonEMRFullAccessPolicy_v2` exige um role com nome especifico `EMR_DefaultRole_V2` e a tag `for-use-with-amazon-emr-managed-policies=true`. Crie esse role:

```bash
# Criar trust policy
cat > /tmp/emr-trust-policy.json << 'EOF'
{
  "Version": "2008-10-17",
  "Statement": [
    {
      "Sid": "",
      "Effect": "Allow",
      "Principal": {
        "Service": "elasticmapreduce.amazonaws.com"
      },
      "Action": "sts:AssumeRole"
    }
  ]
}
EOF

# Criar role V2
aws iam create-role \
    --role-name EMR_DefaultRole_V2 \
    --assume-role-policy-document file:///tmp/emr-trust-policy.json \
    --description "EMR service role v2"

# Anexar policy de servico V2
aws iam attach-role-policy \
    --role-name EMR_DefaultRole_V2 \
    --policy-arn arn:aws:iam::aws:policy/service-role/AmazonEMRServicePolicy_v2
```

### 3.3 Criar service-linked role do EMR

```bash
aws iam create-service-linked-role \
    --aws-service-name elasticmapreduce.amazonaws.com \
    --region REGION
```

### 3.4 Adicionar inline policy ao usuario para PassRole e EC2 tags

A policy v2 tem restricoes de `iam:PassRole` e exige tags nos recursos EC2. Adicione uma inline policy ao usuario:

```bash
cat > /tmp/emr-extra-policy.json << 'EOF'
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Sid": "PassRoleClassicEMR",
      "Effect": "Allow",
      "Action": "iam:PassRole",
      "Resource": [
        "arn:aws:iam::ACCOUNT_ID:role/EMR_DefaultRole",
        "arn:aws:iam::ACCOUNT_ID:role/EMR_DefaultRole_V2",
        "arn:aws:iam::ACCOUNT_ID:role/EMR_EC2_DefaultRole",
        "arn:aws:iam::ACCOUNT_ID:role/EMR_AutoScaling_DefaultRole"
      ]
    },
    {
      "Sid": "EC2TagsForEMR",
      "Effect": "Allow",
      "Action": [
        "ec2:CreateTags",
        "ec2:DescribeTags"
      ],
      "Resource": "*"
    }
  ]
}
EOF

aws iam put-user-policy \
    --user-name MY_IAM_USER \
    --policy-name EMRExtraPermissions \
    --policy-document file:///tmp/emr-extra-policy.json
```

### 3.5 Taguear VPC e subnet para a policy v2

A `AmazonEMRServicePolicy_v2` so permite operacoes em recursos tagueados:

```bash
aws ec2 create-tags \
    --resources VPC_ID SUBNET_ID \
    --tags Key=for-use-with-amazon-emr-managed-policies,Value=true \
    --region REGION
```

### Verificar que os roles foram criados

```bash
aws iam get-role --role-name EMR_DefaultRole_V2 --query "Role.Arn" --output text
aws iam get-role --role-name EMR_EC2_DefaultRole --query "Role.Arn" --output text
```

---

## Passo 4 -- Criar cluster EMR e executar testes

O cluster sera criado com `--auto-terminate`, ou seja, **desligara automaticamente** apos a execucao dos steps. Nao ficara rodando indefinidamente.

### Criar arquivo de configuracao Iceberg

Crie o arquivo `tmp/iceberg-config.json`:

```json
[
  {
    "Classification": "iceberg-defaults",
    "Properties": {
      "iceberg.enabled": "true"
    }
  }
]
```

### Criar cluster com step de teste

> **IMPORTANTE:** Use `--service-role EMR_DefaultRole_V2` e inclua a tag `for-use-with-amazon-emr-managed-policies=true`. O ultimo argumento do step (`MY_BUCKET`) e o nome do bucket passado ao script `test_on_emr.py` (ele usa esse valor quando a variavel `LIB_SPARK_TEST_BUCKET` nao esta definida no ambiente do EMR).

```bash
aws emr create-cluster \
    --name "lib-spark-test" \
    --release-label emr-7.5.0 \
    --applications Name=Spark Name=Hadoop \
    --ec2-attributes SubnetId=SUBNET_ID \
    --instance-groups \
        InstanceGroupType=MASTER,InstanceCount=1,InstanceType=m5.xlarge \
        InstanceGroupType=CORE,InstanceCount=1,InstanceType=m5.xlarge \
    --configurations file://tmp/iceberg-config.json \
    --service-role EMR_DefaultRole_V2 \
    --auto-terminate \
    --log-uri s3://MY_BUCKET/lib-spark/emr-logs/ \
    --tags for-use-with-amazon-emr-managed-policies=true Project=lib-spark Environment=test \
    --steps 'Type=CUSTOM_JAR,Name=lib-spark-test,ActionOnFailure=CONTINUE,Jar=command-runner.jar,Args=[spark-submit,--py-files,s3://MY_BUCKET/lib-spark/artifacts/lib_spark-0.1.0-py3-none-any.whl,--conf,spark.sql.catalog.glue_catalog=org.apache.iceberg.spark.SparkCatalog,--conf,spark.sql.catalog.glue_catalog.catalog-impl=org.apache.iceberg.aws.glue.GlueCatalog,--conf,spark.sql.catalog.glue_catalog.warehouse=s3://MY_BUCKET/lib-spark/warehouse,--conf,spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions,s3://MY_BUCKET/lib-spark/scripts/test_on_emr.py,MY_BUCKET]' \
    --region REGION
```

**Saida esperada:**
```json
{
    "ClusterId": "j-XXXXXXXXXXXXX",
    "ClusterArn": "arn:aws:elasticmapreduce:REGION:ACCOUNT_ID:cluster/j-XXXXXXXXXXXXX"
}
```

Guarde o `ClusterId` para monitoramento.

---

## Passo 5 -- Monitorar execucao

### Acompanhar status do cluster

```bash
# Substituir j-XXXXXXXXXXXXX pelo ClusterId real
aws emr describe-cluster --cluster-id j-XXXXXXXXXXXXX \
    --query "Cluster.{Name:Name,State:Status.State,StateChange:Status.StateChangeReason.Message}" \
    --output table \
    --region REGION
```

**Estados esperados na ordem:**
1. `STARTING` -- Provisionando instancias EC2 (~5 min)
2. `BOOTSTRAPPING` -- Instalando Spark/Iceberg (~3 min)
3. `RUNNING` -- Executando o step de teste (~5 min)
4. `TERMINATING` -- Desligando (auto-terminate)
5. `TERMINATED` -- Finalizado

### Acompanhar status do step

```bash
aws emr list-steps --cluster-id j-XXXXXXXXXXXXX \
    --query "Steps[].{Name:Name,State:Status.State}" \
    --output table \
    --region REGION
```

**Estados do step:**
- `PENDING` -> `RUNNING` -> `COMPLETED` (sucesso)
- `PENDING` -> `RUNNING` -> `FAILED` (falha)

### Ver logs do step

Os logs ficam no S3. Apos o step completar:

```bash
# Listar logs disponveis
aws s3 ls s3://MY_BUCKET/lib-spark/emr-logs/j-XXXXXXXXXXXXX/steps/ \
    --recursive --region REGION

# Baixar stdout do step (substitua o step ID)
aws s3 cp s3://MY_BUCKET/lib-spark/emr-logs/j-XXXXXXXXXXXXX/steps/s-YYYYY/stdout.gz . \
    --region REGION

# Descomprimir e visualizar
gzip -d stdout.gz
cat stdout
```

### Verificar tabelas criadas no Glue

```bash
aws glue get-tables --database-name lib_spark_test \
    --query "TableList[].{Name:Name,Type:Parameters.table_type}" \
    --output table \
    --region REGION
```

---

## Destruicao dos recursos

Execute na ordem inversa para destruir todos os recursos.

### 1. Terminar cluster EMR (se ainda estiver rodando)

```bash
aws emr terminate-clusters --cluster-ids j-XXXXXXXXXXXXX --region REGION
```

### 2. Deletar tabelas Iceberg no Glue

```bash
# Listar tabelas
aws glue get-tables --database-name lib_spark_test \
    --query "TableList[].Name" --output text --region REGION

# Deletar cada tabela
aws glue delete-table --database-name lib_spark_test --name sample_table --region REGION
aws glue delete-table --database-name lib_spark_test --name partitioned_table --region REGION
aws glue delete-table --database-name lib_spark_test --name merge_table --region REGION
aws glue delete-table --database-name lib_spark_test --name evolution_table --region REGION
```

### 3. Deletar database Glue

```bash
aws glue delete-database --name lib_spark_test --region REGION
```

### 4. Remover dados do S3

```bash
# Remover warehouse (dados das tabelas Iceberg)
aws s3 rm s3://MY_BUCKET/lib-spark/ \
    --recursive --region REGION
```

### 5. Remover IAM roles e policies (opcional)

Somente se voce nao pretende usar EMR novamente:

```bash
# Remover policy do EMR_DefaultRole_V2
aws iam detach-role-policy --role-name EMR_DefaultRole_V2 \
    --policy-arn arn:aws:iam::aws:policy/service-role/AmazonEMRServicePolicy_v2
aws iam delete-role --role-name EMR_DefaultRole_V2

# Remover inline policy do usuario
aws iam delete-user-policy --user-name MY_IAM_USER --policy-name EMRExtraPermissions

# Remover tags da VPC/subnet
aws ec2 delete-tags \
    --resources VPC_ID SUBNET_ID \
    --tags Key=for-use-with-amazon-emr-managed-policies \
    --region REGION
```

### 6. Remover permissoes do usuario (opcional)

```bash
aws iam detach-user-policy --user-name MY_IAM_USER \
    --policy-arn arn:aws:iam::aws:policy/AmazonEMRFullAccessPolicy_v2

aws iam detach-user-policy --user-name MY_IAM_USER \
    --policy-arn arn:aws:iam::aws:policy/IAMFullAccess
```

### Script de destruicao completa (copiar e colar)

```bash
# ATENCAO: isso destroi TODOS os recursos do lib-spark.
# Substitua j-XXXXXXXXXXXXX pelo ClusterId real (ou use o ultimo criado).

REGION=sa-east-1
CLUSTER_ID=j-12CGIVXEM2LM7
BUCKET=MY_BUCKET
DB=lib_spark_test

# 1. Terminar cluster (se ainda rodando)
aws emr terminate-clusters --cluster-ids $CLUSTER_ID --region $REGION 2>/dev/null

# 2. Deletar tabelas
for TABLE in sample_table partitioned_table merge_table evolution_table; do
    aws glue delete-table --database-name $DB --name $TABLE --region $REGION 2>/dev/null
done

# 3. Deletar database
aws glue delete-database --name $DB --region $REGION 2>/dev/null

# 4. Remover dados S3
aws s3 rm s3://$BUCKET/lib-spark/ --recursive --region $REGION

# 5. Remover EMR_DefaultRole_V2
aws iam detach-role-policy --role-name EMR_DefaultRole_V2 \
    --policy-arn arn:aws:iam::aws:policy/service-role/AmazonEMRServicePolicy_v2 2>/dev/null
aws iam delete-role --role-name EMR_DefaultRole_V2 2>/dev/null

# 6. Remover inline policy do usuario
aws iam delete-user-policy --user-name MY_IAM_USER --policy-name EMRExtraPermissions 2>/dev/null

# 7. Remover tags da VPC/subnet
aws ec2 delete-tags \
    --resources VPC_ID SUBNET_ID \
    --tags Key=for-use-with-amazon-emr-managed-policies \
    --region $REGION 2>/dev/null

echo "Recursos destruidos."
```

---

## Estimativa de custos

| Recurso | Tipo | Custo estimado | Duracao |
|---------|------|----------------|---------|
| EMR Master | m5.xlarge (4 vCPU, 16 GB) | ~$0.252/h (EC2) + $0.063/h (EMR) | ~15 min |
| EMR Core | m5.xlarge (4 vCPU, 16 GB) | ~$0.252/h (EC2) + $0.063/h (EMR) | ~15 min |
| S3 | Armazenamento + requests | < $0.01 | - |
| Glue Catalog | Metadados | Gratuito (primeiras 1M APIs) | - |

**Total estimado por execucao: ~$0.30** (cluster roda ~15 min e desliga automaticamente).

---

## Resumo dos recursos criados

| Recurso | Nome/ID | Status |
|---------|---------|--------|
| S3 Bucket | `MY_BUCKET` | Definir na sua conta |
| S3 Prefix | `lib-spark/` (artifacts, scripts, warehouse, emr-logs) | Criado |
| Glue Database | `lib_spark_test` | Criado |
| .whl no S3 | `lib-spark/artifacts/lib_spark-0.1.0-py3-none-any.whl` | Uploaded |
| Script no S3 | `lib-spark/scripts/test_on_emr.py` | Uploaded |
| EMR Cluster | `lib-spark-test-v4` (`j-12CGIVXEM2LM7`) | Executado com sucesso (6/6 testes PASSED) |
| EMR_DefaultRole_V2 | IAM Role | Criado |
| EMR service-linked role | `AWSServiceRoleForEMRCleanup` | Criado |
| Inline policy | `EMRExtraPermissions` (user MY_IAM_USER) | Criado |
| VPC/Subnet tags | `for-use-with-amazon-emr-managed-policies=true` | Aplicadas |
| Tabelas Iceberg | `sample_table`, `partitioned_table`, `merge_table`, `evolution_table` | Criadas pelo EMR step |

---

## Testes executados pelo script

O script `test_on_emr.py` executa os seguintes testes na ordem:

| Teste | O que faz |
|-------|-----------|
| APPEND | Insere 3 registros e valida contagem |
| OVERWRITE_TABLE | Substitui toda a tabela e valida que apenas novos registros existem |
| OVERWRITE_PARTITIONS | Reescreve particao 2023 sem afetar 2024 |
| MERGE | Faz upsert com merge_keys=["id"], valida update e insert |
| OPTIMIZATION | Escreve com sort_columns=["id"] e valida optimization_applied=True |
| SCHEMA_EVOLUTION | Adiciona coluna "email" automaticamente via SAFE_SCHEMA_EVOLUTION |
