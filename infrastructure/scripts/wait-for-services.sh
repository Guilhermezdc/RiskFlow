#!/bin/bash
# infrastructure/scripts/wait-for-services.sh
# Aguarda todos os serviços estarem saudáveis antes de continuar

set -e

RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m'

log() { echo -e "${GREEN}[$(date +'%H:%M:%S')]${NC} $1"; }
warn() { echo -e "${YELLOW}[$(date +'%H:%M:%S')]${NC} $1"; }
err() { echo -e "${RED}[$(date +'%H:%M:%S')]${NC} $1"; }

wait_for() {
    local name=$1
    local cmd=$2
    local max_attempts=${3:-30}
    local attempt=0

    warn "Aguardando $name..."
    until eval "$cmd" &>/dev/null; do
        attempt=$((attempt + 1))
        if [ $attempt -ge $max_attempts ]; then
            err "❌ $name não ficou saudável após $max_attempts tentativas"
            exit 1
        fi
        echo -n "."
        sleep 3
    done
    log "✅ $name está pronto"
}

log "🚀 Verificando saúde dos serviços Risk Flow..."
echo ""

# Kafka
wait_for "Kafka" \
    "docker exec rf-kafka kafka-broker-api-versions --bootstrap-server localhost:29092" \
    30

# Redis
wait_for "Redis" \
    "docker exec rf-redis redis-cli ping | grep -q PONG" \
    15

# Cassandra (mais lento para iniciar)
wait_for "Cassandra" \
    "docker exec rf-cassandra cqlsh -e 'SELECT now() FROM system.local'" \
    40

# Flink JobManager
wait_for "Flink JobManager" \
    "curl -sf http://localhost:8081/overview" \
    20

# LocalStack S3
wait_for "LocalStack (AWS)" \
    "curl -sf http://localhost:4566/_localstack/health | grep -q '\"s3\": \"running\"'" \
    20

echo ""
log "🎉 Todos os serviços estão prontos!"
echo ""
echo "  Kafka UI    : http://localhost:8080  (perfil monitoring)"
echo "  Flink UI    : http://localhost:8081"
echo "  Spark UI    : http://localhost:4040  (quando job ativo)"
echo "  Grafana     : http://localhost:3000  (perfil monitoring)"
echo ""
