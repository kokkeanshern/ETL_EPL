/*Identify the latest EPL game*/
SELECT *
FROM games
ORDER BY ID DESC
LIMIT 1;