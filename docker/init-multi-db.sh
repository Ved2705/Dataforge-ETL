#!/bin/bash
set -e

# This script runs as the POSTGRES_USER (dataforge) on first DB init.
# It creates the airflow role + database needed by Airflow services.

echo ">>> Creating airflow role and database..."

psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" <<-EOSQL
    -- Create airflow role only if it doesn't exist
    DO \$\$
    BEGIN
        IF NOT EXISTS (SELECT FROM pg_catalog.pg_roles WHERE rolname = 'airflow') THEN
            CREATE ROLE airflow WITH LOGIN PASSWORD 'airflow';
            RAISE NOTICE 'Role airflow created.';
        ELSE
            RAISE NOTICE 'Role airflow already exists, skipping.';
        END IF;
    END
    \$\$;

    -- Create airflow database only if it doesn't exist
    SELECT 'CREATE DATABASE airflow OWNER airflow'
    WHERE NOT EXISTS (SELECT FROM pg_database WHERE datname = 'airflow')\gexec

    -- Grant all privileges
    GRANT ALL PRIVILEGES ON DATABASE airflow TO airflow;
EOSQL

echo ">>> airflow role and database ready."