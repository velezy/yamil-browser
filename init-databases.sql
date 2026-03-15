-- Create databases for each service (shared Postgres instance)
-- This runs first (00-init-databases.sql) before service-specific schemas.

-- Enable pgvector in template so all new databases get it
CREATE EXTENSION IF NOT EXISTS vector;
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

-- Create databases for each service
SELECT 'CREATE DATABASE yamil_chat' WHERE NOT EXISTS (SELECT FROM pg_database WHERE datname = 'yamil_chat')\gexec
SELECT 'CREATE DATABASE yamil_rag' WHERE NOT EXISTS (SELECT FROM pg_database WHERE datname = 'yamil_rag')\gexec

-- Grant access
GRANT ALL PRIVILEGES ON DATABASE yamil_browser TO yamil;
GRANT ALL PRIVILEGES ON DATABASE yamil_chat TO yamil;
GRANT ALL PRIVILEGES ON DATABASE yamil_rag TO yamil;

-- Enable extensions in each database
\c yamil_chat
CREATE EXTENSION IF NOT EXISTS vector;
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";
CREATE SCHEMA IF NOT EXISTS common;

\c yamil_rag
CREATE EXTENSION IF NOT EXISTS vector;
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";
CREATE SCHEMA IF NOT EXISTS common;

\c yamil_browser
CREATE EXTENSION IF NOT EXISTS vector;
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";
