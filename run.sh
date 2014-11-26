#!/bin/sh
user=study
password=study
db=study00
host=localhost
echo Create user $user for database $db
psql -U postgres -h $host -c "DROP database IF EXISTS $db;"
psql -U postgres -h $host -c "CREATE DATABASE $db;"
psql -U postgres -h $host -c "DROP user IF EXISTS $user;"
psql -U postgres -h $host -c "CREATE USER $user WITH PASSWORD '$password';"
psql -U postgres -h $host -c "GRANT ALL PRIVILEGES ON DATABASE $db to $user;"