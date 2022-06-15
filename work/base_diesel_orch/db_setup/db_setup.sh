#!/bin/bash

for i in `/bin/ls -1 /docker-entrypoint-initdb.d/migrations/*.sql`; do
    psql -f $i
done

for i in `/bin/ls -1 /docker-entrypoint-initdb.d/populate/*.sql`; do
    psql -f $i
done
