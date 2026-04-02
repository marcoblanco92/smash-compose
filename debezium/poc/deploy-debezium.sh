#!/bin/bash

# ============================================================
# Deploy Debezium connector su Kafka Connect
# Eseguire dalla cartella dove si trova debezium-connector-smash.json
# ============================================================

# 1. Registra il connector
echo ">>> Registrazione connector..."
curl -X POST http://localhost:8083/connectors \
  -H "Content-Type: application/json" \
  -d @debezium-connector-smash.json

echo ""
echo ""

# 2. Verifica stato (attendi 3 secondi per il boot)
echo ">>> Attendo 3 secondi..."
sleep 3

echo ">>> Stato connector:"
curl -s http://localhost:8083/connectors/smash-postgres-connector/status | jq

echo ""
echo ""

# 3. Lista topic Kafka (verifica che i 7 topic siano apparsi)
echo ">>> Topic Kafka disponibili:"
curl -s http://localhost:8083/connectors/smash-postgres-connector/topics | jq