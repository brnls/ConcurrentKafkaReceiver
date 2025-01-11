docker compose exec broker kafka-topics --create --topic topic-name --partitions 7 --replication-factor 1 --bootstrap-server localhost:9092
Write-Host "Topic created"

psql -d postgres -U postgres -f .\Tables.sql