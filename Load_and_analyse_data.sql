-- Importation des données
CREATE OR REPLACE DATABASE linkedin;

USE DATABASE linkedin;

CREATE STAGE linkedin_stage
URL='s3://snowflake-lab-bucket/';

create or replace file format csv type='csv'
  compression = 'auto' field_delimiter = ','
  record_delimiter = '\n'  skip_header = 1
  field_optionally_enclosed_by = '\042' trim_space = false
  error_on_column_count_mismatch = false escape = 'none'
  escape_unenclosed_field = '\134'
  date_format = 'auto' timestamp_format = 'auto'
  null_if = ('') comment = 'file format for ingesting csv';

CREATE OR REPLACE FILE FORMAT linkedin_json_format
  TYPE = 'JSON'
  STRIP_OUTER_ARRAY = TRUE;

list @linkedin_stage;

CREATE OR REPLACE TEMPORARY TABLE Temp_Companies (
    company_id VARCHAR,
    name VARCHAR,
    description VARCHAR,
    company_size VARCHAR,
    state VARCHAR,
    country VARCHAR,
    city VARCHAR,
    zip_code VARCHAR,
    adresse VARCHAR,
    url VARCHAR
);

COPY INTO Temp_Companies
FROM @linkedin_stage/companies.csv
FILE_FORMAT = (FORMAT_NAME = csv);

select * from Temp_companies;

CREATE OR REPLACE TABLE Companies (
    company_id STRING,
    name STRING,
    description STRING,
    company_size STRING,
    state STRING,
    country STRING,
    city STRING,
    zip_code STRING,
    adresse STRING,
    url STRING
);


INSERT INTO Companies (
    company_id,
    name,
    description,
    company_size,
    state,
    country,
    city,
    zip_code,
    adresse,
    url)
SELECT
    company_id,
    name,
    description,
    company_size,
    state,
    country,
    city,
    zip_code,
    adresse,
    url
FROM temp_companies;

select * from companies;

CREATE OR REPLACE TABLE Company_Specialities (
    company_id STRING,
    speciality STRING
);

CREATE OR REPLACE TEMPORARY TABLE Temp_Company_Specialities (
    company_id VARCHAR,
    speciality VARCHAR
);

COPY INTO Temp_Company_Specialities
FROM @linkedin_stage/company_specialities.csv
FILE_FORMAT = (FORMAT_NAME = csv);

INSERT INTO Company_Specialities (
    company_id,
    speciality
)
SELECT
    company_id,
    speciality
FROM Temp_Company_Specialities;

select * from temp_company_specialities;


CREATE OR REPLACE TABLE Job_Postings (
    job_id STRING,
    company_id STRING,
    title STRING,
    description STRING,
    max_salary FLOAT,
    med_salary FLOAT,
    min_salary FLOAT,
    pay_period STRING,
    formatted_work_type STRING,
    location STRING,
    applies STRING,
    original_listed_time STRING,
    remote_allowed STRING,
    views STRING,
    job_posting_url STRING,
    application_url STRING,
    application_type STRING,
    expiry STRING,
    closed_time STRING,
    formatted_experience_level STRING,
    skills_desc STRING,
    listed_time STRING,
    posting_domain STRING,
    sponsored STRING,
    work_type STRING,
    currency STRING,
    compensation_type STRING,
    scraped STRING
);

CREATE OR REPLACE TEMPORARY TABLE Temp_Job_Postings (
    job_id VARCHAR,
    company_id VARCHAR,
    title VARCHAR,
    description VARCHAR,
    max_salary VARCHAR,
    med_salary VARCHAR,
    min_salary VARCHAR,
    pay_period VARCHAR,
    formatted_work_type VARCHAR,
    location VARCHAR,
    applies VARCHAR,
    original_listed_time VARCHAR,
    remote_allowed VARCHAR,
    views VARCHAR,
    job_posting_url VARCHAR,
    application_url VARCHAR,
    application_type VARCHAR,
    expiry VARCHAR,
    closed_time VARCHAR,
    formatted_experience_level VARCHAR,
    skills_desc VARCHAR,
    listed_time VARCHAR,
    posting_domain VARCHAR,
    sponsored VARCHAR,
    work_type VARCHAR,
    currency VARCHAR,
    compensation_type VARCHAR,
    scraped VARCHAR
);

COPY INTO Temp_Job_Postings
FROM @linkedin_stage/job_postings.csv
FILE_FORMAT = (FORMAT_NAME = csv);

INSERT INTO Job_Postings (
    job_id,
    company_id,
    title,
    description,
    max_salary,
    med_salary,
    min_salary,
    pay_period,
    formatted_work_type,
    location,
    applies,
    original_listed_time,
    remote_allowed,
    views,
    job_posting_url,
    application_url,
    application_type,
    expiry,
    closed_time,
    formatted_experience_level,
    skills_desc,
    listed_time,
    posting_domain,
    sponsored,
    work_type,
    currency,
    compensation_type,
    scraped
)
SELECT
    job_id,
    company_id,
    title,
    description,
    max_salary,
    med_salary,
    min_salary,
    pay_period,
    formatted_work_type,
    location,
    applies,
    original_listed_time,
    remote_allowed,
    views,
    job_posting_url,
    application_url,
    application_type,
    expiry,
    closed_time,
    formatted_experience_level,
    skills_desc,
    listed_time,
    posting_domain,
    sponsored,
    work_type,
    currency,
    compensation_type,
    scraped
FROM Temp_Job_Postings;

select * from job_postings;

-- Passons au json

CREATE OR REPLACE TABLE Salaries (
    salary_id STRING,
    job_id STRING,
    max_salary FLOAT,
    med_salary FLOAT,
    min_salary FLOAT,
    pay_period STRING,
    currency STRING,
    compensation_type STRING
);

CREATE OR REPLACE TABLE Benefits (
    job_id STRING,
    type STRING,
    inferred STRING
);

CREATE OR REPLACE TABLE Skills (
    skill_abr STRING,
    skill_name STRING
);

CREATE OR REPLACE TABLE Employee_Counts (
    company_id STRING,
    employee_count INT,
    follower_count INT,
    time_recorded STRING
);

CREATE OR REPLACE TABLE Job_Skills (
    job_id STRING,
    skill_abr STRING
);

CREATE OR REPLACE TABLE Industries (
    industry_id STRING,
    industry_name STRING
);

CREATE OR REPLACE TABLE Job_Industries (
    job_id STRING,
    industry_id STRING
);

CREATE OR REPLACE TABLE Company_Industries (
    company_id STRING,
    industry STRING
);

-- Salaries Table
CREATE OR REPLACE TEMPORARY TABLE Temp_Salaries (
    V VARIANT
);

COPY INTO Temp_Salaries
FROM @linkedin_stage/salaries.json
FILE_FORMAT = (FORMAT_NAME = linkedin_json_format);

CREATE OR REPLACE VIEW Salaries_View AS
SELECT
    V:salaries.salary_id::STRING AS salary_id,
    V:salaries.job_id::STRING AS job_id,
    TRY_TO_DOUBLE(V:salaries.max_salary::STRING) AS max_salary,
    TRY_TO_DOUBLE(V:salaries.med_salary::STRING) AS med_salary,
    TRY_TO_DOUBLE(V:salaries.min_salary::STRING) AS min_salary,
    V:salaries.pay_period::STRING AS pay_period,
    V:salaries.currency::STRING AS currency,
    V:salaries.compensation_type::STRING AS compensation_type
FROM Temp_Salaries;

INSERT INTO Salaries
SELECT * FROM Salaries_View;

-- Benefits Table
CREATE OR REPLACE TEMPORARY TABLE Temp_Benefits (
    V VARIANT
);

COPY INTO Temp_Benefits
FROM @linkedin_stage/benefits.json
FILE_FORMAT = (FORMAT_NAME = linkedin_json_format);

CREATE OR REPLACE VIEW Benefits_View AS
SELECT
    V:benefits.job_id::STRING AS job_id,
    V:benefits.type::STRING AS type,
    V:benefits.inferred::STRING AS inferred
FROM Temp_Benefits;

INSERT INTO Benefits
SELECT * FROM Benefits_View;

-- Skills Table
CREATE OR REPLACE TEMPORARY TABLE Temp_Skills (
    V VARIANT
);

COPY INTO Temp_Skills
FROM @linkedin_stage/skills.json
FILE_FORMAT = (FORMAT_NAME = linkedin_json_format);

CREATE OR REPLACE VIEW Skills_View AS
SELECT
    V:skills.skill_abr::STRING AS skill_abr,
    V:skills.skill_name::STRING AS skill_name
FROM Temp_Skills;

INSERT INTO Skills
SELECT * FROM Skills_View;

-- Employee Counts Table
CREATE OR REPLACE TEMPORARY TABLE Temp_Employee_Counts (
    V VARIANT
);

COPY INTO Temp_Employee_Counts
FROM @linkedin_stage/employee_counts.json
FILE_FORMAT = (FORMAT_NAME = linkedin_json_format);

CREATE OR REPLACE VIEW Employee_Counts_View AS
SELECT
    V:employee_counts.company_id::STRING AS company_id,
    TRY_TO_NUMBER(V:employee_counts.employee_count::STRING) AS employee_count,
    TRY_TO_NUMBER(V:employee_counts.follower_count::STRING) AS follower_count,
    V:employee_counts.time_recorded::STRING AS time_recorded
FROM Temp_Employee_Counts;

INSERT INTO Employee_Counts
SELECT * FROM Employee_Counts_View;

-- Job Skills Table
CREATE OR REPLACE TEMPORARY TABLE Temp_Job_Skills (
    V VARIANT
);

COPY INTO Temp_Job_Skills
FROM @linkedin_stage/job_skills.json
FILE_FORMAT = (FORMAT_NAME = linkedin_json_format);

CREATE OR REPLACE VIEW Job_Skills_View AS
SELECT
    V:job_skills.job_id::STRING AS job_id,
    V:job_skills.skill_abr::STRING AS skill_abr
FROM Temp_Job_Skills;

INSERT INTO Job_Skills
SELECT * FROM Job_Skills_View;

-- Industries Table
CREATE OR REPLACE TEMPORARY TABLE Temp_Industries (
    V VARIANT
);

COPY INTO Temp_Industries
FROM @linkedin_stage/industries.json
FILE_FORMAT = (FORMAT_NAME = linkedin_json_format);

CREATE OR REPLACE VIEW Industries_View AS
SELECT
    V:industry_id::STRING AS industry_id,
    V:industry_name::STRING AS industry_name
FROM Temp_Industries;

INSERT INTO Industries
SELECT * FROM Industries_View;

-- Job Industries Table
CREATE OR REPLACE TEMPORARY TABLE Temp_Job_Industries (
    V VARIANT
);

COPY INTO Temp_Job_Industries
FROM @linkedin_stage/job_industries.json
FILE_FORMAT = (FORMAT_NAME = linkedin_json_format);

CREATE OR REPLACE VIEW Job_Industries_View AS
SELECT
    V:job_industries.job_id::STRING AS job_id,
    V:job_industries.industry_id::STRING AS industry_id
FROM Temp_Job_Industries;

INSERT INTO Job_Industries
SELECT * FROM Job_Industries_View;

-- Company Industries Table
CREATE OR REPLACE TEMPORARY TABLE Temp_Company_Industries (
    V VARIANT
);

COPY INTO Temp_Company_Industries
FROM @linkedin_stage/company_industries.json
FILE_FORMAT = (FORMAT_NAME = linkedin_json_format);

CREATE OR REPLACE VIEW Company_Industries_View AS
SELECT
    V:company_industries.company_id::STRING AS company_id,
    V:company_industries.industry::STRING AS industry
FROM Temp_Company_Industries;

INSERT INTO Company_Industries
SELECT * FROM Company_Industries_View;

-- Validate Data Insertion
SELECT * FROM Salaries LIMIT 10;
SELECT * FROM Benefits LIMIT 10;
SELECT * FROM Skills LIMIT 10;
SELECT * FROM Employee_Counts LIMIT 10;
SELECT * FROM Job_Skills LIMIT 10;
SELECT * FROM Industries LIMIT 10;
SELECT * FROM Job_Industries LIMIT 10;
SELECT * FROM Company_Industries LIMIT 10;

-- Salaries Table
CREATE OR REPLACE TEMPORARY TABLE Temp_Salaries (
    V VARIANT
);

COPY INTO Temp_Salaries
FROM @linkedin_stage/salaries.json
FILE_FORMAT = (FORMAT_NAME = linkedin_json_format);

CREATE OR REPLACE VIEW Salaries_View AS
SELECT
    v:"salary_id"::STRING AS salary_id,
    v:"job_id"::STRING AS job_id,
    TRY_TO_DOUBLE(v:"max_salary"::STRING) AS max_salary,
    TRY_TO_DOUBLE(v:"med_salary"::STRING) AS med_salary,
    TRY_TO_DOUBLE(v:"min_salary"::STRING) AS min_salary,
    v:"pay_period"::STRING AS pay_period,
    v:"currency"::STRING AS currency,
    v:"compensation_type"::STRING AS compensation_type
FROM Temp_Salaries;

INSERT INTO Salaries
SELECT * FROM Salaries_View;

-- Benefits Table
CREATE OR REPLACE TEMPORARY TABLE Temp_Benefits (
    V VARIANT
);

COPY INTO Temp_Benefits
FROM @linkedin_stage/benefits.json
FILE_FORMAT = (FORMAT_NAME = linkedin_json_format);

CREATE OR REPLACE VIEW Benefits_View AS
SELECT
    v:"job_id"::STRING AS job_id,
    v:"type"::STRING AS type,
    v:"inferred"::STRING AS inferred
FROM Temp_Benefits;

INSERT INTO Benefits
SELECT * FROM Benefits_View;

-- Skills Table
CREATE OR REPLACE TEMPORARY TABLE Temp_Skills (
    V VARIANT
);

COPY INTO Temp_Skills
FROM @linkedin_stage/skills.json
FILE_FORMAT = (FORMAT_NAME = linkedin_json_format);

CREATE OR REPLACE VIEW Skills_View AS
SELECT
    v:"skill_abr"::STRING AS skill_abr,
    v:"skill_name"::STRING AS skill_name
FROM Temp_Skills;

INSERT INTO Skills
SELECT * FROM Skills_View;

-- Employee Counts Table
CREATE OR REPLACE TEMPORARY TABLE Temp_Employee_Counts (
    V VARIANT
);

COPY INTO Temp_Employee_Counts
FROM @linkedin_stage/employee_counts.json
FILE_FORMAT = (FORMAT_NAME = linkedin_json_format);

CREATE OR REPLACE VIEW Employee_Counts_View AS
SELECT
    v:"company_id"::STRING AS company_id,
    TRY_TO_NUMBER(v:"employee_count"::STRING) AS employee_count,
    TRY_TO_NUMBER(v:"follower_count"::STRING) AS follower_count,
    v:"time_recorded"::STRING AS time_recorded
FROM Temp_Employee_Counts;

INSERT INTO Employee_Counts
SELECT * FROM Employee_Counts_View;

-- Job Skills Table
CREATE OR REPLACE TEMPORARY TABLE Temp_Job_Skills (
    V VARIANT
);

COPY INTO Temp_Job_Skills
FROM @linkedin_stage/job_skills.json
FILE_FORMAT = (FORMAT_NAME = linkedin_json_format);

CREATE OR REPLACE VIEW Job_Skills_View AS
SELECT
    v:"job_id"::STRING AS job_id,
    v:"skill_abr"::STRING AS skill_abr
FROM Temp_Job_Skills;

INSERT INTO Job_Skills
SELECT * FROM Job_Skills_View;

-- Industries Table
CREATE OR REPLACE TEMPORARY TABLE Temp_Industries (
    V VARIANT
);

COPY INTO Temp_Industries
FROM @linkedin_stage/industries.json
FILE_FORMAT = (FORMAT_NAME = linkedin_json_format);

CREATE OR REPLACE VIEW Industries_View AS
SELECT
    v:"industry_id"::STRING AS industry_id,
    v:"industry_name"::STRING AS industry_name
FROM Temp_Industries;

INSERT INTO Industries
SELECT * FROM Industries_View;

-- Job Industries Table
CREATE OR REPLACE TEMPORARY TABLE Temp_Job_Industries (
    V VARIANT
);

COPY INTO Temp_Job_Industries
FROM @linkedin_stage/job_industries.json
FILE_FORMAT = (FORMAT_NAME = linkedin_json_format);

CREATE OR REPLACE VIEW Job_Industries_View AS
SELECT
    v:"job_id"::STRING AS job_id,
    v:"industry_id"::STRING AS industry_id
FROM Temp_Job_Industries;

INSERT INTO Job_Industries
SELECT * FROM Job_Industries_View;

-- Company Industries Table
CREATE OR REPLACE TEMPORARY TABLE Temp_Company_Industries (
    V VARIANT
);

COPY INTO Temp_Company_Industries
FROM @linkedin_stage/company_industries.json
FILE_FORMAT = (FORMAT_NAME = linkedin_json_format);

CREATE OR REPLACE VIEW Company_Industries_View AS
SELECT
    v:"company_id"::STRING AS company_id,
    v:"industry"::STRING AS industry
FROM Temp_Company_Industries;

INSERT INTO Company_Industries
SELECT * FROM Company_Industries_View;

-- Test de nos tables issues des fichiers json
SELECT * FROM Salaries_View LIMIT 10;
SELECT * FROM Benefits_View LIMIT 10;
SELECT * FROM Skills_View LIMIT 10;
SELECT * FROM Employee_Counts_View LIMIT 10;
SELECT * FROM Job_Skills_View LIMIT 10;
SELECT * FROM Industries_View LIMIT 10;
SELECT * FROM Job_Industries_View LIMIT 10;
SELECT * FROM Company_Industries_View LIMIT 10;
-- Tout marche 

-- Analyse des données
-- Question 1 

SELECT title, COUNT(*) AS job_count
FROM job_postings
GROUP BY title
ORDER BY job_count DESC
LIMIT 10;

-- Question 2
-- On vérifie combien de currency on a 

SELECT DISTINCT currency 
FROM salaries_view;

-- Etrangement juste une (USD)

WITH Normalized_Salaries AS (
    SELECT
        JOB_ID,
        CASE
            WHEN PAY_PERIOD = 'HOURLY' THEN MAX_SALARY * 40 * 52
            WHEN PAY_PERIOD = 'MONTHLY' THEN MAX_SALARY * 12
            WHEN PAY_PERIOD = 'YEARLY' THEN MAX_SALARY
            ELSE NULL
        END AS ANNUAL_MAX_SALARY
    FROM
        salaries_view
),
Combined_Data AS (
    SELECT
        jp.TITLE,
        ns.ANNUAL_MAX_SALARY
    FROM
        job_postings jp
    JOIN
        Normalized_Salaries ns
    ON
        jp.JOB_ID = ns.JOB_ID
)
SELECT
    TITLE,
    ANNUAL_MAX_SALARY
FROM
    Combined_Data
WHERE
    ANNUAL_MAX_SALARY IS NOT NULL
ORDER BY
    ANNUAL_MAX_SALARY DESC
LIMIT 10;

-- Question 3
SELECT c.company_size, COUNT(*) AS job_count
FROM job_postings jp
JOIN companies c ON jp.company_id = c.company_id
GROUP BY c.company_size
ORDER BY job_count DESC;

-- Question 4
SELECT i.industry_name, COUNT(ji.job_id) AS nombre_offres
FROM Job_Industries ji
JOIN Industries i ON ji.industry_id = i.industry_id
GROUP BY i.industry_name
ORDER BY nombre_offres DESC
LIMIT 20;

-- Question 5
SELECT formatted_work_type, COUNT(job_id) AS nombre_offres
FROM JOB_POSTINGS
GROUP BY formatted_work_type
ORDER BY nombre_offres DESC;

-- Question 6
SELECT location, COUNT(job_id) AS nombre_offres
FROM Job_postings
GROUP BY location
ORDER BY nombre_offres DESC;
