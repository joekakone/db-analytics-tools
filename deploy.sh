#!usr/bin/bash

docker compose down -v

docker compose build --no-cache

docker compose up -d


# docker stop job_runner

# docker rm job_runner

# docker rmi job_webapp

# # docker build --no-cache -f Dockerfile.ui -t job_webapp .

# docker run -p 8050:8050 \
#     --restart=always \
#     --name job_runner \
#     --network=deploy-data-warehouse_analytics-network \
#     -d job_webapp
