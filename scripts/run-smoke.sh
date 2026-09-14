#!/usr/bin/env bash
# Ingests the official HotpotQA Scott Derrickson / Ed Wood example and runs HippoRAG 1.
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
# shellcheck source=env.sh
source "$ROOT/scripts/env.sh"
cd "$ROOT"

EMBEDDING_URI="${MONACGRAPH_EMBEDDING_URI:-http://127.0.0.1:8099/}"
EXTRACTOR_URL="${MONACGRAPH_EXTRACTOR_URL:-http://127.0.0.1:11434/v1/chat/completions}"
MODEL="${MONACGRAPH_LLM_MODEL:-qwen3:0.6b}"
DB_NAME="${MONACGRAPH_DB:-hotpotqa-demo-$(date +%Y%m%d%H%M%S)}"
INPUT="$ROOT/data/hotpotqa-smoke/scott-derrickson-ed-wood.txt"
QUESTION="Were Scott Derrickson and Ed Wood of the same nationality?"
JAR="$ROOT/target/Gremmunity-1.0-SNAPSHOT.jar"

embedding_health="${EMBEDDING_URI%/}/health"
if ! curl -sf --max-time 3 "$embedding_health" >/dev/null; then
  cat >&2 <<EOF
Embedding service is not reachable at $embedding_health
Start it first:

  ./scripts/start-embedding.sh
EOF
  exit 1
fi

if ! curl -sf --max-time 3 "http://127.0.0.1:11434/api/tags" >/dev/null; then
  cat >&2 <<EOF
Ollama is not reachable at http://127.0.0.1:11434
Install Ollama, then:

  ollama pull $MODEL
EOF
  exit 1
fi

if [[ ! -f "$JAR" ]]; then
  echo "Packaging Java application (mvn package)"
  mvn -q -DskipTests package
fi

echo "Using new graph --db $DB_NAME (do not reopen an empty-start JNI graph)"
exec java -cp "$JAR" db.monacgraph.ingestion.IngestionCli \
  --db "$DB_NAME" \
  --input "$INPUT" \
  --extractor-url "$EXTRACTOR_URL" \
  --extractor-model "$MODEL" \
  --embedding-uri "$EMBEDDING_URI" \
  --query "$QUESTION"
