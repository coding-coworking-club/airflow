SELECT id, title, description, start_time, end_time, create_time, last_update_time
FROM contest
WHERE title LIKE '%2022 Fall%'
ORDER BY id ASC;
