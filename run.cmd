@echo off
set user="study"
set password="study"
set db="study00"
echo Create user %user% for database %db%
psql -U postgres -c "DROP database %db%;"
psql -U postgres -c "CREATE DATABASE %db%;"
psql -U postgres -c "DROP user %user%;"
psql -U postgres -c "CREATE USER %user% WITH PASSWORD '%password%';"
psql -U postgres -c "GRANT ALL PRIVILEGES ON DATABASE %db% to %user%;"