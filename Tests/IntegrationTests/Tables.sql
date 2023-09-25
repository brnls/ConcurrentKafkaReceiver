drop table if exists results;
drop table if exists logs;
create table results(
    id bigint primary key generated always as identity,
    host text not null,
    message_id text not null,
    topic text not null,
    t_partition int not null,
    t_offset int not null);

create table logs(
    id bigint primary key generated always as identity,
    host text not null,
    message text not null,
    created_at timestamptz default now());

select count(*), host from results group by host;
select count(*), host, topic from results group by host, topic;

select count(*), topic, t_partition, t_offset from results group by topic, t_partition, t_offset having count(*) > 1;
select count(*), message_id from results group by message_id having count(*) > 1;
select host, message from logs order by host, id asc;


select count(*), max(t_offset), t_partition from results group by t_partition;
select t_partition, t_offset from results order by t_partition, t_offset;


WITH ranked_data AS (
    SELECT
        t_partition,
        t_offset,
        LAG(t_offset) OVER (PARTITION BY t_partition ORDER BY t_offset) AS prev_offset
    FROM
        results
)

SELECT
    t_partition,
    CASE
        WHEN COUNT(*) = 1 THEN 'Gapless'
        WHEN MAX(t_offset - prev_offset) = 1 THEN 'Gapless'
        ELSE 'Not Gapless'
    END AS result
FROM
    ranked_data
GROUP BY
    t_partition;