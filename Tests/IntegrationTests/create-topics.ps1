docker compose exec broker kafka-topics --create --topic topic-name --partitions 7 --replication-factor 1 --bootstrap-server localhost:9092
Write-Host "Topic created"

# Example SQL string
$sqlString = "CREATE TABLE example_table (id SERIAL PRIMARY KEY, name VARCHAR(100));"

# Run the SQL string with psql
psql -c "select count(*), topic, t_partition, t_offset from results group by topic, t_partition, t_offset having count(*) > 1;"
psql -c "select count(*), host from results group by host;"
psql -c "select count(*), host, topic from results group by host, topic;"
psql -c "select host, message from logs order by host, id asc;"
psql -c "select jsonb_agg(value::jsonb) from stats;" | Out-File -FilePath "stats.json"