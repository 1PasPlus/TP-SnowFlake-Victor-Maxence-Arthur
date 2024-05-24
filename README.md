# TP-SnowFlake-Victor-Maxence-Arthur

## 1.	Chargement des données

### 1.1	Création de la database Linkedin
CREATE OR REPLACE DATABASE linkedin;

USE DATABASE linkedin;  # Connexion à la database

### 1.2	Création du stage pour spécifier l’emplacement du bucket

CREATE STAGE linkedin_stage
URL='s3://snowflake-lab-bucket/';

### 1.3	Création du format de fichier
 
create or replace file format csv type='csv'
  compression = 'auto' field_delimiter = ','
  record_delimiter = '\n'  skip_header = 1.   
  field_optionally_enclosed_by = '\042' trim_space = false. 
  error_on_column_count_mismatch = false escape = 'none' 
  escape_unenclosed_field = '\134'
  date_format = 'auto' timestamp_format = 'auto'
  null_if = ('') comment = 'file format for ingesting csv';

create or replace file format linkedin_json_format
  type = ‘json’
  strip_outer_array = true;
 

### 1.4	Création des différentes tables

-- Pour récupérer le nom et de type des fichiers nous faisons cette commande : 

list @linkedin_stage;

 
-- Grâce à cela, nous pouvons déterminer la méthode à employer pour mettre les données dans des tables. Dans un premier temps, nous créons une table temporaire avec tous les -- éléments en type varchar pour ne pas avoir de problème lors de l’insersion. 

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

-- Après avoir récupéré les données, on applique la méthode pour les formats csv pour insérer les données dans la table temporaire

COPY INTO Temp_Companies
FROM @linkedin_stage/companies.csv
FILE_FORMAT = (FORMAT_NAME = csv);

-- Enfin on crée la table finale avec le bon type de donnée que l’on a pu vérifier en regardant les premières lignes de la table temporaire 

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

-- Maintenant plus qu’a les insérer 

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

-- Même chose pour la table Compagny_Specialities :

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

-- Même chose pour la table Job_posting

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

-- Insersion des fichiers json

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

### 1.5	Modification des tables pour utiliser les données

-- Pour la table salaries, on s’est rendu compte que certains salaires sont reseingnés à l’heure et d’autres en salaire annuel. De ce faite, on fait : 

CREATE OR REPLACE VIEW Salaries_View_Updated AS
SELECT
    salary_id,
    job_id,
    CASE WHEN pay_period = 'HOURLY' THEN max_salary * 2080 ELSE max_salary END AS max_salary,
    CASE WHEN pay_period = 'HOURLY' THEN med_salary * 2080 ELSE med_salary END AS med_salary,
    CASE WHEN pay_period = 'HOURLY' THEN min_salary * 2080 ELSE min_salary END AS min_salary,
    CASE WHEN pay_period = 'HOURLY' THEN 'YEARLY' ELSE pay_period END AS pay_period,
    currency,
    compensation_type
FROM Salaries_View;

-- Insérer les données mises à jour dans la table Salaries
INSERT INTO Salaries (salary_id, job_id, max_salary, med_salary, min_salary, pay_period, currency, compensation_type)
SELECT
    salary_id,
    job_id,
    max_salary,
    med_salary,
    min_salary,
    pay_period,
    currency,
    compensation_type
FROM Salaries_View_Updated
WHERE pay_period = 'YEARLY';
## 2.	Analyse des données

### 2.1 Top 10 des jobs les plus postés

-- Question 1

SELECT title, COUNT(*) AS job_count
FROM jobs_posting
GROUP BY title
ORDER BY job_count DESC
LIMIT 10;

-- Ici on veut afficher les intitulé et le nombre de poste correspondant

-- On va chercher dans la bonne table et on veut un classement par ordre croissant du nombre de job par post
-- On veut uniquement les 10 premiers


<img width="454" alt="image" src="https://github.com/1PasPlus/TP-SnowFlake-Victor-Maxence-Arthur/assets/163517694/b5d61ac2-9192-4949-b828-dde2973f47aa">

 
 

### 2.2	Classement des jobs les mieux payés par intitulé de poste

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

<img width="454" alt="image" src="https://github.com/1PasPlus/TP-SnowFlake-Victor-Maxence-Arthur/assets/163517694/456ce5bb-95cf-47e6-9444-9f18ab3bc82e">

-- Le salaire max est très élevé mais cela vient probablement du fait que il y a des cases vides dans le dataset que certains salaires marqués en 'USD' se trouvent en fait être -- des salaire dans d'autres monnaies. Le dataset n'est pas très fiable 

### 2.3	Quelle est la répartition des offres d’emploi par taille d’entreprise ?

-- Question 3

SELECT c.company_size, COUNT(*) AS job_count
FROM job_postings jp
JOIN companies c ON jp.company_id = c.company_id
GROUP BY c.company_size
ORDER BY job_count DESC;

<img width="454" alt="image" src="https://github.com/1PasPlus/TP-SnowFlake-Victor-Maxence-Arthur/assets/163517694/2a88c4b1-e194-4c95-ab72-5f82c7183a12">


### 2.4	Quelle est la répartition des offres d’emploi par type d’industrie ?

-- Question 4

SELECT i.industry_name, COUNT(ji.job_id) AS nombre_offres
FROM Job_Industries ji
JOIN Industries i ON ji.industry_id = i.industry_id
GROUP BY i.industry_name
ORDER BY nombre_offres DESC
LIMIT 20;

<img width="454" alt="image" src="https://github.com/1PasPlus/TP-SnowFlake-Victor-Maxence-Arthur/assets/163517694/155a51d6-111d-44be-a901-f304f12c966d">

### 2.5 Quelle est la réparation des offres d’emploi par type d’emploi (full-time, intership, part-time) ?

-- Question 5

SELECT formatted_work_type, COUNT(job_id) AS nombre_offres
FROM JOB_POSTINGS
GROUP BY formatted_work_type
ORDER BY nombre_offres DESC;

<img width="454" alt="image" src="https://github.com/1PasPlus/TP-SnowFlake-Victor-Maxence-Arthur/assets/163517694/1b280536-2be7-4621-a504-dd3f3e6cd4ce">


### 2.6	Suggestion d’analyse sur le nombre d’offres d’emploi par endroit 

-- Question 6

SELECT location, COUNT(job_id) AS nombre_offres
FROM Job_postings
GROUP BY location
ORDER BY nombre_offres DESC
LIMIT 20;

<img width="454" alt="image" src="https://github.com/1PasPlus/TP-SnowFlake-Victor-Maxence-Arthur/assets/163517694/27195efe-072d-458f-a5b8-8d59ed34b629">


