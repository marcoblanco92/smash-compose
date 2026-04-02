#!/bin/bash

# ============================================================
# Stop TUTTI i connectors Debezium
# Uso: ./stop-all-connectors.sh
# ============================================================

CONNECT_URL="http://localhost:8083"

echo ">>> Connectors attivi:"
CONNECTORS=$(curl -s "$CONNECT_URL/connectors" | jq -r '.[]')
echo "$CONNECTORS"
echo ""

if [ -z "$CONNECTORS" ]; then
  echo ">>> Nessun connector attivo."
  exit 0
fi

echo ">>> Rimozione di tutti i connectors..."
echo ""

for name in $CONNECTORS; do
  RESPONSE=$(curl -s -o /dev/null -w "%{http_code}" -X DELETE "$CONNECT_URL/connectors/$name")
  if [ "$RESPONSE" -eq 204 ]; then
    echo "  ✓ $name rimosso"
  elif [ "$RESPONSE" -eq 404 ]; then
    echo "  - $name non trovato"
  else
    echo "  ✗ $name errore HTTP $RESPONSE"
  fi
done

echo ""
sleep 2
echo ">>> Verifica — connectors rimasti:"
curl -s "$CONNECT_URL/connectors" | jq