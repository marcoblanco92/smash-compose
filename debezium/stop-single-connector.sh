#!/bin/bash

# ============================================================
# Stop singolo connector Debezium
# Uso: ./stop-connector.sh <nome-connector>
# Esempio: ./stop-connector.sh smash-cbs-connector
# ============================================================

CONNECT_URL="http://localhost:8083"

if [ -z "$1" ]; then
  echo "Uso: $0 <nome-connector>"
  echo ""
  echo "Connectors disponibili:"
  curl -s "$CONNECT_URL/connectors" | jq -r '.[]'
  exit 1
fi

CONNECTOR_NAME="$1"

echo ">>> Stato attuale di $CONNECTOR_NAME:"
curl -s "$CONNECT_URL/connectors/$CONNECTOR_NAME/status" | jq '{connector: .connector.state, task: .tasks[0].state}'
echo ""

echo ">>> Rimozione $CONNECTOR_NAME..."
RESPONSE=$(curl -s -o /dev/null -w "%{http_code}" -X DELETE "$CONNECT_URL/connectors/$CONNECTOR_NAME")

if [ "$RESPONSE" -eq 204 ]; then
  echo ">>> $CONNECTOR_NAME rimosso con successo."
elif [ "$RESPONSE" -eq 404 ]; then
  echo ">>> $CONNECTOR_NAME non trovato — già rimosso?"
else
  echo ">>> Errore: HTTP $RESPONSE"
fi

echo ""
echo ">>> Connectors rimasti attivi:"
curl -s "$CONNECT_URL/connectors" | jq