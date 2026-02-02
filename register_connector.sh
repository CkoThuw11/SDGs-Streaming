#!/bin/bash
echo "Waiting for Debezium to be ready..."
# Loop until the Debezium API responds with 200 OK
while [[ "$(curl -s -o /dev/null -w ''%{http_code}'' http://localhost:8083/connectors)" != "200" ]]; do
    printf '.'
    sleep 5
done
echo " Debezium is ready!"

echo "Registering connector..."
# Submit the configuration file
curl -i -X POST -H "Accept:application/json" -H "Content-Type:application/json" http://localhost:8083/connectors/ -d @debezium-connector.json

echo ""
echo "Current Connectors:"
curl -s http://localhost:8083/connectors/
