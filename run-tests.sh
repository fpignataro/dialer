#!/usr/bin/env sh

if [ -z "$1" ]; then
    docker-compose --env-file .env-tests -f docker-compose-test.yml up -d --build --remove-orphans
else
    docker-compose --env-file .env-tests -f docker-compose-test.yml up -d --remove-orphans
fi

docker ps

docker exec omnidialer-worker-test sh -c "python -m unittest tests.py --failfast; exit \$?"

# docker-compose --env-file .env-tests -f docker-compose-test.yml down
