CONNECT_URL="http://localhost:8083"

echo ">>> Connectors attivi:"
CONNECTORS=$(curl -s "$CONNECT_URL/connectors" | jq -r '.[]')
echo "$CONNECTORS"
echo ""

if [ -z "$CONNECTORS" ]; then
  echo ">>> Nessun connector attivo."
  exit 0
fi