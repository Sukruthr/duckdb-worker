SELECT
    i % 1000 AS group_id,
    count(*) AS row_count,
    sum(i) AS total
FROM range(1000000) AS events(i)
GROUP BY group_id;
