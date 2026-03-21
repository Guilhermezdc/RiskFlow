#!/bin/bash
# infrastructure/scripts/init-schemas.sh
# Inicializa os schemas do Cassandra e Redshift (local: LocalStack não tem Redshift, apenas log)

set -e

GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m'

log() { echo -e "${GREEN}[$(date +'%H:%M:%S')]${NC} $1"; }
warn() { echo -e "${YELLOW}[$(date +'%H:%M:%S')]${NC} $1"; }

PROJECT_ROOT="$(cd "$(dirname "$0")/../.." && pwd)"

# ── Cassandra Schema ──
log "⚙️ Aplicando schema no Cassandra..."
docker exec -i rf-cassandra cqlsh < "$PROJECT_ROOT/fraud-engine/cassandra/schema.cql"
log "✅ Schema Cassandra aplicado"

# ── Redshift DDL (apenas exibe em ambiente local) ──
if [ "${REDSHIFT_HOST:-}" != "" ]; then
    log "⚙️ Aplicando DDLs no Redshift..."
    psql \
        "host=${REDSHIFT_HOST} \
         port=${REDSHIFT_PORT:-5439} \
         dbname=${REDSHIFT_DB} \
         user=${REDSHIFT_USER} \
         password=${REDSHIFT_PASSWORD}" \
        -f "$PROJECT_ROOT/analytics/redshift/schema.sql"
    log "✅ Schema Redshift aplicado"
else
    warn "⚠️ REDSHIFT_HOST não configurado — pulando DDLs do Redshift"
    warn "   Para ambientes locais, use LocalStack ou um Redshift real"
    warn "   Execute manualmente: psql <conn_string> -f analytics/redshift/schema.sql"
fi

log "🎉 Schemas inicializados com sucesso!"
