# scripts/build_and_run.sh
#!/bin/bash

# Build Docker image
docker build -t etl_app .

# Run Docker container
docker run -d -p 5000:5000 --name etl_container etl_app

# Wait for the container to start
sleep 5

# Run the ETL process by triggering the API endpoint
./trigger_etl.sh

# Query the database and showcase that it has been populated with the desired features
./query_database.sh
