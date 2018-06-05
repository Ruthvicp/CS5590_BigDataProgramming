USE test;
CREATE TABLE employees (
	employee_id int,
	department text,
	lastname text,
	years_with_company int,
	PRIMARY KEY(department, lastname, years_with_company)
);

COPY employees (employee_id, department, lastname, years_with_company) FROM 'employee_entries.csv' WITH HEADER=True AND DELIMITER='|';