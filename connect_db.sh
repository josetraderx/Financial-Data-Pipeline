#!/bin/bash
# Script para conectar fácilmente a PostgreSQL
echo "🗄️  Conectando a PostgreSQL Exodus DB..."
psql -h localhost -p 5433 -U josetraderx -d exodus_db
