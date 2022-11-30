COPY courses_contest(course_id, type, judge_contest_id, name, start_time, end_time, create_time, update_time)
FROM STDIN
WITH DELIMITER ',' 
CSV HEADER;
