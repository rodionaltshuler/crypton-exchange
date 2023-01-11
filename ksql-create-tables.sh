docker exec -it ksqldb-server ksql-migrations --config-file /share/ksql-migrations/ksql-migrations.properties initialize-metadata

docker exec -it ksqldb-server ksql-migrations --config-file /share/ksql-migrations/ksql-migrations.properties apply --all
