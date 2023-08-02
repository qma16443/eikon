# scripts/query_database.sh
#!/bin/bash

# Connect to PostgreSQL database and run queries
docker exec -it etl_container psql -h localhost -U your_username -d your_database_name -c "SELECT * FROM user_features;"
