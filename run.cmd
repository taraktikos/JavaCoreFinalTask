@echo off
set user="study"
set password="study"
set db="study00"
echo Create user %user% for database %db%
psql -U postgres -c "DROP database IF EXISTS %db%;"
psql -U postgres -c "DROP user IF EXISTS %user%;"
psql -U postgres -c "CREATE DATABASE %db%;"
psql -U postgres -c "CREATE USER %user% WITH PASSWORD '%password%';"
psql -U postgres -c "GRANT ALL PRIVILEGES ON DATABASE %db% to %user%;"
psql -U postgres -d %db% -f ./src/sql/schema.sql
psql -U postgres -c "GRANT postgres TO %user%;"