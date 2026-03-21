#!/bin/bash
# infrastructure/docker/localstack/init-aws.sh
# Inicializa recursos AWS no LocalStack

echo "⚙️ Inicializando recursos AWS (LocalStack)..."

export AWS_DEFAULT_REGION=us-east-1
export AWS_ACCESS_KEY_ID=test
export AWS_SECRET_ACCESS_KEY=test
export ENDPOINT=http://localhost:4566

# ── S3 Buckets ──
echo "📦 Criando buckets S3..."

awslocal s3 mb s3://risk-flow-datalake
awslocal s3api put-bucket-versioning \
  --bucket risk-flow-datalake \
  --versioning-configuration Status=Enabled

# Estrutura de prefixos (simula partições)
awslocal s3api put-object --bucket risk-flow-datalake --key raw/transactions/.keep --body /dev/null
awslocal s3api put-object --bucket risk-flow-datalake --key raw/decisions/.keep --body /dev/null
awslocal s3api put-object --bucket risk-flow-datalake --key checkpoints/.keep --body /dev/null
awslocal s3api put-object --bucket risk-flow-datalake --key redshift-temp/.keep --body /dev/null

echo "✅ Bucket 'risk-flow-datalake' criado"

# ── SNS Topics ──
echo "📣 Criando tópicos SNS..."

awslocal sns create-topic --name risk-flow-decisions
awslocal sns create-topic --name risk-flow-alerts

echo "✅ Tópicos SNS criados"

# ── SQS Queues ──
echo "📬 Criando filas SQS..."

awslocal sqs create-queue --queue-name decisions-output \
  --attributes '{"VisibilityTimeout":"30","MessageRetentionPeriod":"86400"}'

awslocal sqs create-queue --queue-name manual-review \
  --attributes '{"VisibilityTimeout":"60","MessageRetentionPeriod":"259200"}'

echo "✅ Filas SQS criadas"

# ── Listagem final ──
echo ""
echo "📋 Recursos criados:"
echo "S3 Buckets:"
awslocal s3 ls

echo ""
echo "SNS Topics:"
awslocal sns list-topics

echo ""
echo "SQS Queues:"
awslocal sqs list-queues

echo ""
echo "✅ LocalStack inicializado com sucesso!"
