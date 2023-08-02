# scripts/trigger_etl.sh
#!/bin/bash

# Trigger ETL process by calling the API endpoint
curl -X POST http://localhost:5000/trigger_etl
