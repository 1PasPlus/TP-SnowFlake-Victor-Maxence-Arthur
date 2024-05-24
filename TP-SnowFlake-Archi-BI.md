TP Architecture BI

24/05/2024

Victor Asencio, Maxence Brunet, Arhtur Mehats

![Une image contenant symbole, capture d’écran, logo, Graphique

Description générée automatiquement](Aspose.Words.815cfa07-ed5d-4169-892f-fd0db2454a3e.001.png)











1. # Chargement des données

1. ### Création de la database Linkedin
CREATE OR REPLACE DATABASE linkedin;

USE DATABASE linkedin;  *# Connexion à la database*

1. ### Création du stage pour spécifier l’emplacement du bucket

CREATE STAGE linkedin\_stage

URL='s3://snowflake-lab-bucket/';

1. ### Création du format de fichier

14


![](Aspose.Words.815cfa07-ed5d-4169-892f-fd0db2454a3e.002.png)create or replace file format csv type='csv'

`  `compression = 'auto' field\_delimiter = ','

`  `record\_delimiter = '\n'  skip\_header = 1.   

`  `field\_optionally\_enclosed\_by = '\042' trim\_space = false.* 

`  `error\_on\_column\_count\_mismatch = false escape = 'none'* 

`  `escape\_unenclosed\_field = '\134'

`  `date\_format = 'auto' timestamp\_format = 'auto'

`  `null\_if = ('') comment = 'file format for ingesting csv';

create or replace file format linkedin\_json\_format

`  `type = ‘json’

`  `strip\_outer\_array = true;


1. ### Création des différentes tables
*Pour récupérer le nom et de type des fichiers nous faisons cette commande :* 

list @linkedin\_stage;

![Une image contenant texte, capture d’écran, Police, nombre

Description générée automatiquement](Aspose.Words.815cfa07-ed5d-4169-892f-fd0db2454a3e.003.png)

*Grâce à cela, nous pouvons déterminer la méthode à employer pour mettre les données dans des tables. Dans un premier temps, nous créons une table temporaire avec tous les éléments en type varchar pour ne pas avoir de problème lors de l’insersion.* 

CREATE OR REPLACE TEMPORARY TABLE Temp\_Companies (

`    `company\_id VARCHAR,

`    `name VARCHAR,

`    `description VARCHAR,

`    `company\_size VARCHAR,

`    `state VARCHAR,

`    `country VARCHAR,

`    `city VARCHAR,

`    `zip\_code VARCHAR,

`    `adresse VARCHAR,

`    `url VARCHAR

);

*Après avoir récupéré les données, on applique la méthode pour les formats csv pour insérer les données dans la table temporaire*

COPY INTO Temp\_Companies

FROM @linkedin\_stage/companies.csv

FILE\_FORMAT = (FORMAT\_NAME = csv);

*Enfin on crée la table finale avec le bon type de donnée que l’on a pu vérifier en regardant les premières lignes de la table temporaire* 

CREATE OR REPLACE TABLE Companies (

`    `company\_id STRING,

`    `name STRING,

`    `description STRING,

`    `company\_size STRING,

`    `state STRING,

`    `country STRING,

`    `city STRING,

`    `zip\_code STRING,

`    `adresse STRING,

`    `url STRING

);

*Maintenant plus qu’a les insérer* 

INSERT INTO Companies (

`    `company\_id,

`    `name,

`    `description,

`    `company\_size,

`    `state,

`    `country,

`    `city,

`    `zip\_code,

`    `adresse,

`    `url)

SELECT

`    `company\_id,

`    `name,

`    `description,

`    `company\_size,

`    `state,

`    `country,

`    `city,

`    `zip\_code,

`    `adresse,

`    `url

FROM temp\_companies;

*Même chose pour la table Compagny\_Specialities :*

CREATE OR REPLACE TABLE Company\_Specialities (

`    `company\_id STRING,

`    `speciality STRING

);

CREATE OR REPLACE TEMPORARY TABLE Temp\_Company\_Specialities (

`    `company\_id VARCHAR,

`    `speciality VARCHAR

);

COPY INTO Temp\_Company\_Specialities

FROM @linkedin\_stage/company\_specialities.csv

FILE\_FORMAT = (FORMAT\_NAME = csv);

INSERT INTO Company\_Specialities (

`    `company\_id,

`    `speciality

)

SELECT

`    `company\_id,

`    `speciality

FROM Temp\_Company\_Specialities;

*Même chose pour la table Job\_posting*

CREATE OR REPLACE TABLE Job\_Postings (

`    `job\_id STRING,

`    `company\_id STRING,

`    `title STRING,

`    `description STRING,

`    `max\_salary FLOAT,

`    `med\_salary FLOAT,

`    `min\_salary FLOAT,

`    `pay\_period STRING,

`    `formatted\_work\_type STRING,

`    `location STRING,

`    `applies STRING,

`    `original\_listed\_time STRING,

`    `remote\_allowed STRING,

`    `views STRING,

`    `job\_posting\_url STRING,

`    `application\_url STRING,

`    `application\_type STRING,

`    `expiry STRING,

`    `closed\_time STRING,

`    `formatted\_experience\_level STRING,

`    `skills\_desc STRING,

`    `listed\_time STRING,

`    `posting\_domain STRING,

`    `sponsored STRING,

`    `work\_type STRING,

`    `currency STRING,

`    `compensation\_type STRING,

`    `scraped STRING

);

CREATE OR REPLACE TEMPORARY TABLE Temp\_Job\_Postings (

`    `job\_id VARCHAR,

`    `company\_id VARCHAR,

`    `title VARCHAR,

`    `description VARCHAR,

`    `max\_salary VARCHAR,

`    `med\_salary VARCHAR,

`    `min\_salary VARCHAR,

`    `pay\_period VARCHAR,

`    `formatted\_work\_type VARCHAR,

`    `location VARCHAR,

`    `applies VARCHAR,

`    `original\_listed\_time VARCHAR,

`    `remote\_allowed VARCHAR,

`    `views VARCHAR,

`    `job\_posting\_url VARCHAR,

`    `application\_url VARCHAR,

`    `application\_type VARCHAR,

`    `expiry VARCHAR,

`    `closed\_time VARCHAR,

`    `formatted\_experience\_level VARCHAR,

`    `skills\_desc VARCHAR,

`    `listed\_time VARCHAR,

`    `posting\_domain VARCHAR,

`    `sponsored VARCHAR,

`    `work\_type VARCHAR,

`    `currency VARCHAR,

`    `compensation\_type VARCHAR,

`    `scraped VARCHAR

);

COPY INTO Temp\_Job\_Postings

FROM @linkedin\_stage/job\_postings.csv

FILE\_FORMAT = (FORMAT\_NAME = csv);

INSERT INTO Job\_Postings (

`    `job\_id,

`    `company\_id,

`    `title,

`    `description,

`    `max\_salary,

`    `med\_salary,

`    `min\_salary,

`    `pay\_period,

`    `formatted\_work\_type,

`    `location,

`    `applies,

`    `original\_listed\_time,

`    `remote\_allowed,

`    `views,

`    `job\_posting\_url,

`    `application\_url,

`    `application\_type,

`    `expiry,

`    `closed\_time,

`    `formatted\_experience\_level,

`    `skills\_desc,

`    `listed\_time,

`    `posting\_domain,

`    `sponsored,

`    `work\_type,

`    `currency,

`    `compensation\_type,

`    `scraped

)

SELECT

`    `job\_id,

`    `company\_id,

`    `title,

`    `description,

`    `max\_salary,

`    `med\_salary,

`    `min\_salary,

`    `pay\_period,

`    `formatted\_work\_type,

`    `location,

`    `applies,

`    `original\_listed\_time,

`    `remote\_allowed,

`    `views,

`    `job\_posting\_url,

`    `application\_url,

`    `application\_type,

`    `expiry,

`    `closed\_time,

`    `formatted\_experience\_level,

`    `skills\_desc,

`    `listed\_time,

`    `posting\_domain,

`    `sponsored,

`    `work\_type,

`    `currency,

`    `compensation\_type,

`    `scraped

FROM Temp\_Job\_Postings;

*# Insersion des fichiers json*

CREATE OR REPLACE TABLE Salaries (

`    `salary\_id STRING,

`    `job\_id STRING,

`    `max\_salary FLOAT,

`    `med\_salary FLOAT,

`    `min\_salary FLOAT,

`    `pay\_period STRING,

`    `currency STRING,

`    `compensation\_type STRING

);



CREATE OR REPLACE TABLE Benefits (

`    `job\_id STRING,

`    `type STRING,

`    `inferred STRING

);



CREATE OR REPLACE TABLE Skills (

`    `skill\_abr STRING,

`    `skill\_name STRING

);



CREATE OR REPLACE TABLE Employee\_Counts (

`    `company\_id STRING,

`    `employee\_count INT,

`    `follower\_count INT,

`    `time\_recorded STRING

);



CREATE OR REPLACE TABLE Job\_Skills (

`    `job\_id STRING,

`    `skill\_abr STRING

);



CREATE OR REPLACE TABLE Industries (

`    `industry\_id STRING,

`    `industry\_name STRING

);



CREATE OR REPLACE TABLE Job\_Industries (

`    `job\_id STRING,

`    `industry\_id STRING

);



CREATE OR REPLACE TABLE Company\_Industries (

`    `company\_id STRING,

`    `industry STRING

);

-- Salaries Table

CREATE OR REPLACE TEMPORARY TABLE Temp\_Salaries (

`    `V VARIANT

);

COPY INTO Temp\_Salaries

FROM @linkedin\_stage/salaries.json

FILE\_FORMAT = (FORMAT\_NAME = linkedin\_json\_format);

CREATE OR REPLACE VIEW Salaries\_View AS

SELECT

`    `v:"salary\_id"::STRING AS salary\_id,

`    `v:"job\_id"::STRING AS job\_id,

`    `TRY\_TO\_DOUBLE(v:"max\_salary"::STRING) AS max\_salary,

`    `TRY\_TO\_DOUBLE(v:"med\_salary"::STRING) AS med\_salary,

`    `TRY\_TO\_DOUBLE(v:"min\_salary"::STRING) AS min\_salary,

`    `v:"pay\_period"::STRING AS pay\_period,

`    `v:"currency"::STRING AS currency,

`    `v:"compensation\_type"::STRING AS compensation\_type

FROM Temp\_Salaries;

INSERT INTO Salaries

SELECT \* FROM Salaries\_View;

-- Benefits Table

CREATE OR REPLACE TEMPORARY TABLE Temp\_Benefits (

`    `V VARIANT

);

COPY INTO Temp\_Benefits

FROM @linkedin\_stage/benefits.json

FILE\_FORMAT = (FORMAT\_NAME = linkedin\_json\_format);

CREATE OR REPLACE VIEW Benefits\_View AS

SELECT

`    `v:"job\_id"::STRING AS job\_id,

`    `v:"type"::STRING AS type,

`    `v:"inferred"::STRING AS inferred

FROM Temp\_Benefits;

INSERT INTO Benefits

SELECT \* FROM Benefits\_View;

-- Skills Table

CREATE OR REPLACE TEMPORARY TABLE Temp\_Skills (

`    `V VARIANT

);

COPY INTO Temp\_Skills

FROM @linkedin\_stage/skills.json

FILE\_FORMAT = (FORMAT\_NAME = linkedin\_json\_format);

CREATE OR REPLACE VIEW Skills\_View AS

SELECT

`    `v:"skill\_abr"::STRING AS skill\_abr,

`    `v:"skill\_name"::STRING AS skill\_name

FROM Temp\_Skills;

INSERT INTO Skills

SELECT \* FROM Skills\_View;

-- Employee Counts Table

CREATE OR REPLACE TEMPORARY TABLE Temp\_Employee\_Counts (

`    `V VARIANT

);

COPY INTO Temp\_Employee\_Counts

FROM @linkedin\_stage/employee\_counts.json

FILE\_FORMAT = (FORMAT\_NAME = linkedin\_json\_format);

CREATE OR REPLACE VIEW Employee\_Counts\_View AS

SELECT

`    `v:"company\_id"::STRING AS company\_id,

`    `TRY\_TO\_NUMBER(v:"employee\_count"::STRING) AS employee\_count,

`    `TRY\_TO\_NUMBER(v:"follower\_count"::STRING) AS follower\_count,

`    `v:"time\_recorded"::STRING AS time\_recorded

FROM Temp\_Employee\_Counts;

INSERT INTO Employee\_Counts

SELECT \* FROM Employee\_Counts\_View;

-- Job Skills Table

CREATE OR REPLACE TEMPORARY TABLE Temp\_Job\_Skills (

`    `V VARIANT

);

COPY INTO Temp\_Job\_Skills

FROM @linkedin\_stage/job\_skills.json

FILE\_FORMAT = (FORMAT\_NAME = linkedin\_json\_format);

CREATE OR REPLACE VIEW Job\_Skills\_View AS

SELECT

`    `v:"job\_id"::STRING AS job\_id,

`    `v:"skill\_abr"::STRING AS skill\_abr

FROM Temp\_Job\_Skills;

INSERT INTO Job\_Skills

SELECT \* FROM Job\_Skills\_View;

-- Industries Table

CREATE OR REPLACE TEMPORARY TABLE Temp\_Industries (

`    `V VARIANT

);

COPY INTO Temp\_Industries

FROM @linkedin\_stage/industries.json

FILE\_FORMAT = (FORMAT\_NAME = linkedin\_json\_format);

CREATE OR REPLACE VIEW Industries\_View AS

SELECT

`    `v:"industry\_id"::STRING AS industry\_id,

`    `v:"industry\_name"::STRING AS industry\_name

FROM Temp\_Industries;

INSERT INTO Industries

SELECT \* FROM Industries\_View;

-- Job Industries Table

CREATE OR REPLACE TEMPORARY TABLE Temp\_Job\_Industries (

`    `V VARIANT

);

COPY INTO Temp\_Job\_Industries

FROM @linkedin\_stage/job\_industries.json

FILE\_FORMAT = (FORMAT\_NAME = linkedin\_json\_format);

CREATE OR REPLACE VIEW Job\_Industries\_View AS

SELECT

`    `v:"job\_id"::STRING AS job\_id,

`    `v:"industry\_id"::STRING AS industry\_id

FROM Temp\_Job\_Industries;

INSERT INTO Job\_Industries

SELECT \* FROM Job\_Industries\_View;

-- Company Industries Table

CREATE OR REPLACE TEMPORARY TABLE Temp\_Company\_Industries (

`    `V VARIANT

);

COPY INTO Temp\_Company\_Industries

FROM @linkedin\_stage/company\_industries.json

FILE\_FORMAT = (FORMAT\_NAME = linkedin\_json\_format);

CREATE OR REPLACE VIEW Company\_Industries\_View AS

SELECT

`    `v:"company\_id"::STRING AS company\_id,

`    `v:"industry"::STRING AS industry

FROM Temp\_Company\_Industries;

INSERT INTO Company\_Industries

SELECT \* FROM Company\_Industries\_View;

1. # Analyse des données

1. ### Top 10 des jobs les plus postés


SELECT title, COUNT(\*) AS job\_count

FROM jobs\_posting

GROUP BY title

ORDER BY job\_count DESC

LIMIT 10;




*Ici on veut afficher les intitulé et le nombre de poste correspondant*

*On va chercher dans la bonne table et on veut un classement par ordre croissant du nombre de job par post*
\*


*On veut uniquement les 10 premiers*



![Résultats](Aspose.Words.815cfa07-ed5d-4169-892f-fd0db2454a3e.004.png)

1. ### Classement des jobs les mieux payés par intitulé de poste


Requete pour la question 2:

Creation d’une table exchange rate:

CREATE TABLE exchange\_rates (

`    `currency STRING PRIMARY KEY,

`    `exchange\_rate\_to\_euro NUMBER

);

--Remplir la table avec les currency actualisées :

INSERT INTO exchange\_rates (currency, exchange\_rate\_to\_euro)

VALUES ('USD', 1.12),

`       `('GBP', 0.87),

`       `('JPY', 124.50),

`       `('CAD', 1.47);

--Creation d’une vue pour changer les salaires en euro :

CREATE OR REPLACE VIEW salaries\_in\_euro AS

SELECT s.salary\_id, s.job\_id, s.max\_salary \* er.exchange\_rate\_to\_euro AS max\_salary\_eur

FROM salaries s

JOIN exchange\_rates er ON s.currency = er.currency;


--Requête finale 

SELECT jp.title, MAX(se.max\_salary\_eur) AS max\_salary\_eur

FROM jobs\_posting jp

JOIN salaries\_in\_euro se ON jp.job\_id = se.job\_id

GROUP BY jp.title

ORDER BY max\_salary\_eur DESC;

1. ### Quelle est la répartition des offres d’emploi par taille d’entreprise ?

-- Question 3

SELECT c.company\_size, COUNT(\*) AS job\_count

FROM job\_postings jp

JOIN companies c ON jp.company\_id = c.company\_id

GROUP BY c.company\_size

ORDER BY job\_count DESC;

![Une image contenant capture d’écran, texte, logiciel, nombre

Description générée automatiquement](Aspose.Words.815cfa07-ed5d-4169-892f-fd0db2454a3e.005.png)







1. ### Quelle est la répartition des offres d’emploi par type d’industrie ?

-- Question 4

SELECT i.industry\_name, COUNT(ji.job\_id) AS nombre\_offres

FROM Job\_Industries ji

JOIN Industries i ON ji.industry\_id = i.industry\_id

GROUP BY i.industry\_name

ORDER BY nombre\_offres DESC

LIMIT 20;

![Une image contenant texte, nombre, capture d’écran

Description générée automatiquement](Aspose.Words.815cfa07-ed5d-4169-892f-fd0db2454a3e.006.png)

Requête question 5 : 

SELECT formatted\_work\_type, COUNT(job\_id) AS nombre\_offres

FROM Jobs\_posting

GROUP BY formatted\_work\_type

ORDER BY nombre\_offres DESC;

Requête pourla question 6 :

SELECT location, COUNT(job\_id) AS nombre\_offres

FROM Jobs\_posting

GROUP BY location

ORDER BY nombre\_offres DESC;

