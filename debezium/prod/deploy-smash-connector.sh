#!/bin/bash

# ============================================================
# Deploy 4 Debezium connectors per dominio
# Eseguire dalla cartella: smash-compose/debezium/prod/
# ============================================================

CONNECT_URL="http://localhost:8083"

# -------------------------------------------------------
# 1. Deploy dei 4 connector per dominio
# -------------------------------------------------------
CONNECTORS=("smash-cbs-connector" "smash-crm-connector" "smash-digital-connector" "smash-market-connector")
FILES=("smash-cbs-connector.json" "smash-crm-connector.json" "smash-crm-connector.json" "smash-market-connector.json")

echo ">>> Registrazione smash-cbs-connector..."
curl -s -X POST "$CONNECT_URL/connectors" \
  -H "Content-Type: application/json" \
  -d @smash-cbs-connector.json
echo ""

echo ">>> Registrazione smash-crm-connector..."
curl -s -X POST "$CONNECT_URL/connectors" \
  -H "Content-Type: application/json" \
  -d @smash-crm-connector.json
echo ""

echo ">>> Registrazione smash-digital-connector..."
curl -s -X POST "$CONNECT_URL/connectors" \
  -H "Content-Type: application/json" \
  -d @smash-digital-connector.json
echo ""

echo ">>> Registrazione smash-market-connector..."
curl -s -X POST "$CONNECT_URL/connectors" \
  -H "Content-Type: application/json" \
  -d @smash-market-connector.json
echo ""

# -------------------------------------------------------
# 2. Attendi avvio
# -------------------------------------------------------
echo ">>> Attendo 5 secondi per il boot..."
sleep 5

# -------------------------------------------------------
# 3. Verifica stato di tutti i connector
# -------------------------------------------------------
echo ">>> Stato connectors:"
echo ""

for name in "${CONNECTORS[@]}"; do
  echo "--- $name ---"
  curl -s "$CONNECT_URL/connectors/$name/status" | jq '{connector: .connector.state, task: .tasks[0].state}'
  echo ""
done

# -------------------------------------------------------
# 4. Lista topic creati
# -------------------------------------------------------
echo ">>> Topic per connector:"
echo ""

for name in "${CONNECTORS[@]}"; do
  echo "--- $name ---"
  curl -s "$CONNECT_URL/connectors/$name/topics" | jq
  echo ""
done