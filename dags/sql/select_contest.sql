SELECT id, title, description, start_time, end_time, create_time, last_update_time
FROM contest
WHERE title LIKE 'ccClub 2022 Fall%' OR title like '2022秋%';
