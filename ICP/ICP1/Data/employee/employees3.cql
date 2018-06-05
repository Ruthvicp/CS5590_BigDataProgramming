USE test;
CREATE TABLE employees3 (
	employee_id int,
	department text,
	firstname text,
	lastname text,
	gender text,
	b_mon text,
	b_day int,
	b_year int,
	PRIMARY KEY(employee_id, gender)
);

COPY employees3 (employee_id, department, firstname, lastname, gender, b_mon, b_day, b_year) FROM 'employee_entries2.csv' WITH HEADER=True AND DELIMITER='|';