CREATE DATABASE BANKING;

-- SHOW TABLES;
-- desc table account;

CREATE OR REPLACE TABLE DISTRICT(
District_Code INT PRIMARY KEY	,
District_Name VARCHAR(100)	,
Region VARCHAR(100)	,
No_of_inhabitants	INT,
No_of_municipalities_with_inhabitants_less_499 INT,
No_of_municipalities_with_inhabitants_500_btw_1999	INT,
No_of_municipalities_with_inhabitants_2000_btw_9999	INT,
No_of_municipalities_with_inhabitants_less_10000 INT,	
No_of_cities	INT,
Ratio_of_urban_inhabitants	FLOAT,
Average_salary	INT,
No_of_entrepreneurs_per_1000_inhabitants	INT,
No_committed_crime_2017	INT,
No_committed_crime_2018 INT
) ;

CREATE OR REPLACE TABLE ACCOUNT(
account_id INT PRIMARY KEY,
district_id	INT,
frequency	VARCHAR(40),
Date DATE ,
Account_type VARCHAR(40),
Card_assigned VARCHAR(40),
FOREIGN KEY (district_id) references DISTRICT(District_Code) 
);

CREATE  OR REPLACE TABLE ORDER_LIST(
order_id	INT PRIMARY KEY,
account_id	INT,
bank_to	VARCHAR(45),
account_to	INT,
amount FLOAT,
FOREIGN KEY (account_id) references ACCOUNT(account_id)
);

CREATE  OR REPLACE TABLE LOAN(
loan_id	INT ,
account_id	INT,
Date	DATE,
amount	INT,
duration	INT,
payments	INT,
status VARCHAR(35),
FOREIGN KEY (account_id) references ACCOUNT(account_id)
);

CREATE  OR REPLACE TABLE TRANSACTIONS(
trans_id INT,	
account_id	INT,
Date	DATE,
Type	VARCHAR(30),
operation	VARCHAR(40),
amount	INT,
balance	FLOAT,
Purpose	VARCHAR(40),
bank	VARCHAR(45),
account INT,
FOREIGN KEY (account_id) references ACCOUNT(account_id));

CREATE  OR REPLACE TABLE CLIENT(
client_id	INT PRIMARY KEY,
Birth_date	DATE,
district_id INT,
Sex	CHAR(10),
FOREIGN KEY (district_id) references DISTRICT(District_Code) 
);

CREATE  OR REPLACE TABLE DISPOSITION(
disp_id	INT PRIMARY KEY,
client_id INT,
account_id	INT,
type CHAR(15),
FOREIGN KEY (account_id) references ACCOUNT(account_id),
FOREIGN KEY (client_id) references CLIENT(client_id)
);

CREATE  OR REPLACE TABLE CARD(
card_id	INT PRIMARY KEY,
disp_id	INT,
type CHAR(10)	,
issued DATE,
FOREIGN KEY (disp_id) references DISPOSITION(disp_id)
);

--------------------------------------------------------------------------------------------------------
----------------------------------------------- FILE FORMAT --------------------------------------------

CREATE OR REPLACE FILE FORMAT CSV_CZECH
TYPE ='CSV'
SKIP_HEADER =1
FIELD_DELIMITER = ','
RECORD_DELIMITER = '\n' ;

SHOW FILE FORMATS;
DESC FILE FORMAT CSV_CZECH;

---------------------------------------------------------------------------------------------------------
--------------------------------------------- STORAGE INTEGRATION ---------------------------------------

CREATE OR REPLACE STORAGE INTEGRATION S3_INT_CZECH
TYPE = EXTERNAL_STAGE
STORAGE_PROVIDER = S3
ENABLED = TRUE
STORAGE_AWS_ROLE_ARN = '{Your Role AWS Role ARN}'
STORAGE_ALLOWED_LOCATIONS = ('s3://{Your Bucket Name}/');

DESC INTEGRATION S3_INT_CZECH;
SHOW STORAGE INTEGRATIONS;

-- drop integration s3_int_czech;
----------------------------------------------------------------------------------------------------------
------------------------------------------------------ STAGE ---------------------------------------------

CREATE OR REPLACE STAGE STAGE_CZECH
URL = 's3://{Your Bucket Name}'
STORAGE_INTEGRATION = S3_INT_CZECH
FILE_FORMAT = CSV_CZECH ;

SHOW STAGES;
DESC STAGE STAGE_CZECH;

LIST @STAGE_CZECH;

-- drop stage stage_czech;
-----------------------------------------------------------------------------------------------------------
------------------------------------------------------- PIPE ----------------------------------------------

CREATE OR REPLACE PIPE SNOWPIPE_ACCOUNT AUTO_INGEST = TRUE AS
COPY INTO BANKING.PUBLIC.ACCOUNT
FROM @STAGE_CZECH/Account/
FILE_FORMAT = CSV_CZECH;

CREATE OR REPLACE PIPE SNOWPIPE_CARD AUTO_INGEST = TRUE AS 
COPY INTO BANKING.PUBLIC.CARD
FROM @STAGE_CZECH/Card/
FILE_FORMAT = CSV_CZECH;

CREATE OR REPLACE PIPE SNOWPIPE_CLIENT AUTO_INGEST = TRUE AS 
COPY INTO BANKING.PUBLIC.CLIENT
FROM @STAGE_CZECH/Client/
FILE_FORMAT = CSV_CZECH;

CREATE OR REPLACE PIPE SNOWPIPE_DISPOSITION AUTO_INGEST = TRUE AS 
COPY INTO BANKING.PUBLIC.DISPOSITION
FROM @STAGE_CZECH/Disposition/
FILE_FORMAT = CSV_CZECH;

CREATE OR REPLACE PIPE SNOWPIPE_DISTRICT AUTO_INGEST = TRUE AS
COPY INTO BANKING.PUBLIC.DISTRICT
FROM @STAGE_CZECH/District/
FILE_FORMAT = CSV_CZECH;

CREATE OR REPLACE PIPE SNOWPIPE_LOAN AUTO_INGEST = TRUE AS
COPY INTO BANKING.PUBLIC.LOAN
FROM @STAGE_CZECH/Loan/
FILE_FORMAT = CSV_CZECH;

CREATE OR REPLACE PIPE SNOWPIPE_ORDER AUTO_INGEST = TRUE AS
COPY INTO BANKING.PUBLIC.ORDER_LIST
FROM @STAGE_CZECH/Order/
FILE_FORMAT = CSV_CZECH;

CREATE OR REPLACE PIPE SNOWPIPE_TRANSACTION AUTO_INGEST = TRUE AS
COPY INTO BANKING.PUBLIC.TRANSACTIONS
FROM @STAGE_CZECH/Transaction
FILE_FORMAT = CSV_CZECH;

SHOW PIPES;

-----------------------------------------------------------------------------------------------
--------------------------------------- REFRESH PIPE -----------------------------------------

ALTER PIPE SNOWPIPE_ACCOUNT REFRESH;
ALTER PIPE SNOWPIPE_CARD REFRESH;
ALTER PIPE SNOWPIPE_CLIENT REFRESH;
ALTER PIPE SNOWPIPE_DISPOSITION REFRESH;
ALTER PIPE SNOWPIPE_DISTRICT REFRESH;
ALTER PIPE SNOWPIPE_LOAN REFRESH;
ALTER PIPE SNOWPIPE_ORDER REFRESH;
ALTER PIPE SNOWPIPE_TRANSACTION REFRESH;

-------------------------------------------------------------------------------------------------

SELECT COUNT(*) FROM ACCOUNT;
SELECT COUNT(*) FROM CARD;
SELECT COUNT(*) FROM CLIENT;
SELECT COUNT(*) FROM DISPOSITION;
SELECT COUNT(*) FROM DISTRICT;
SELECT COUNT(*) FROM LOAN;
SELECT COUNT(*) FROM ORDER_LIST;
SELECT COUNT(*) FROM TRANSACTIONS;

-- DELETE FROM TRANSACTIONS;

-- select (date('2023-12-31') - date('2021-12-31'))/365;

ALTER TABLE CLIENT
ADD COLUMN AGE INT;

SELECT * FROM CLIENT LIMIT 20;

UPDATE CLIENT 
SET AGE = YEAR(DATE('2022-12-31')) - YEAR(BIRTH_DATE);

-- SELECT YEAR(SELECT MAX(DATE) FROM TRANSACTIONS) - YEAR(BIRTH_DATE) FROM CLIENT;

SELECT * FROM TRANSACTIONS WHERE BANK IS NULL AND YEAR(DATE) = '2016';

SELECT YEAR(DATE), 
COUNT(*) AS TOTAL_TXNS 
FROM TRANSACTIONS 
-- WHERE BANK IS NULL
GROUP BY 1
ORDER BY 1 DESC;

SELECT DISTINCT YEAR(DATE) FROM TRANSACTIONS;
SELECT * FROM TRANSACTIONS WHERE YEAR(DATE) = '2019' AND BANK IS NULL;

-- SELECT YEAR(DATE('2022-12-31'))
-------------------------------------------------------------------------------------------------------------------------------------
-------------------------------------------------------------------------------------------------------------------------------------

-- NEXT DATA TRANSFORMATION
/*
    CONVERT 2021 TO 2022
    CONVERT 2020 TO 2021
    CONVERT 2019 TO 2020
    CONVERT 2018 TO 2019
    CONVERT 2017 TO 2018
    CONVERT 2016 TO 2017  
 */

UPDATE TRANSACTIONS
SET DATE = DATEADD(YEAR, 1, DATE)
WHERE YEAR(DATE) = '2021';

UPDATE TRANSACTIONS
SET DATE  = DATEADD(YEAR, 1, DATE)
WHERE YEAR(DATE) = '2020';

UPDATE TRANSACTIONS
SET DATE = DATEADD(YEAR, 1, DATE)
WHERE YEAR(DATE) = '2019';

UPDATE TRANSACTIONS
SET DATE = DATEADD(YEAR, 1, DATE)
WHERE YEAR(DATE) = '2018';

UPDATE TRANSACTIONS
SET DATE = DATEADD(YEAR, 1, DATE)
WHERE YEAR(DATE) = '2017' ;

UPDATE TRANSACTIONS
SET DATE = DATEADD(YEAR, 1, DATE)
WHERE YEAR(DATE) = '2016';

-------------------------------------------------------------------------------------------------------------------------------------
-------------------------------------------------------------------------------------------------------------------------------------

SELECT * FROM TRANSACTIONS
WHERE BANK IS NULL AND YEAR(DATE) = '2022';

UPDATE TRANSACTIONS
SET BANK = 'SKY Bank' 
WHERE BANK IS NULL 
AND YEAR(DATE) = '2022';

-------------------------------------------

SELECT * FROM TRANSACTIONS
WHERE BANK IS NULL AND YEAR(DATE) = '2021';

UPDATE TRANSACTIONS
SET BANK = 'DBS Bank'
WHERE BANK IS NULL
AND YEAR(DATE) = '2021';

-------------------------------------------

SELECT * FROM TRANSACTIONS
WHERE BANK IS NULL AND YEAR(DATE) = '2020';

-------------------------------------------

SELECT * FROM TRANSACTIONS
WHERE BANK IS NULL AND YEAR(DATE) = '2019';

UPDATE TRANSACTIONS
SET BANK = 'Northern Bank'
WHERE BANK IS NULL 
AND YEAR(DATE) = '2019';

-------------------------------------------

SELECT * FROM TRANSACTIONS
WHERE BANK IS NULL AND YEAR(DATE) = '2018';

UPDATE TRANSACTIONS
SET BANK = 'Southern Bank'
WHERE BANK IS NULL
AND YEAR(DATE) = '2018';

-------------------------------------------

SELECT * FROM TRANSACTIONS
WHERE BANK IS NULL AND YEAR(DATE) = '2017';

UPDATE TRANSACTIONS
SET BANK = 'ADB Bank'
WHERE BANK IS NULL
AND YEAR(DATE) = '2017';

-------------------------------------------

UPDATE CARD
SET ISSUED = DATEADD(YEAR,1,ISSUED);

-------------------------------------------

SELECT DISTINCT YEAR(DATE) FROM TRANSACTIONS ORDER BY 1 DESC;
SELECT DISTINCT YEAR(ISSUED) FROM CARD;
SELECT DISTINCT YEAR(DATE),COUNT(*) FROM ACCOUNT GROUP BY 1;

SELECT * FROM CLIENT LIMIT 10;

-- COUNT THE NO OF FEMALE AND MALE CLIENT
SELECT
SUM(CASE WHEN SEX = 'Male' THEN 1 ELSE 0 END) AS NO_MALE_CLIENT,
SUM(CASE WHEN SEX = 'Female' THEN 1 ELSE 0 END) AS NO_FEMALE_CLIENT
FROM CLIENT;
-- SAME AS ABOVE BUT IN ROW FORMAT
SELECT SEX,COUNT(SEX)
FROM CLIENT
GROUP BY 1;

-- WHAT IS MALE AND FEMALE RATIO / %
SELECT 
COUNT(SEX),
SUM(CASE SEX WHEN 'Male' THEN 1 ELSE 0 END) AS MALE_COUNT,
SUM(CASE SEX WHEN 'Male' THEN 1 ELSE 0 END)/ COUNT(SEX) *100 AS MALE_RATIO,
SUM(CASE SEX WHEN 'Female' THEN 1 ELSE 0 END) AS FEMALE_COUNT,
SUM(CASE SEX WHEN 'Female' THEN 1 ELSE 0 END) / COUNT(SEX) *100 AS FEMALE_RATIO
FROM CLIENT;

----------------------------------------------------------------------------------------------------------------------------------------
-- 1. What is the demographic profile of the bank's clients and how does it vary across districts?
CREATE OR REPLACE TABLE CZECH_DEMOGRAPHICS_DATA AS
SELECT C.DISTRICT_ID, D.DISTRICT_NAME, D.AVERAGE_SALARY,
ROUND(AVG(C.AGE),0) AS AVG_AGE,
SUM(CASE C.SEX WHEN 'Male' THEN 1 ELSE 0 END) AS MALE_CLIENT,
SUM(CASE C.SEX WHEN 'Female' THEN 1 ELSE 0 END) AS FEMALE_CLIENT,
SUM(CASE C.SEX WHEN 'Male' THEN 1 ELSE 0 END)/COUNT(*)*100.0 AS MALE_PERC,
SUM(CASE C.SEX WHEN 'Female' THEN 1 ELSE 0 END)/COUNT(8)*100.0 AS FEMALE_PERC,
ROUND(FEMALE_PERC/MALE_PERC *100.0,2) AS FEMALE_MALE_RATION,
COUNT(*) AS TOTAL_CLIENT
FROM CLIENT AS C
LEFT JOIN DISTRICT AS D 
ON C.DISTRICT_ID = D.DISTRICT_CODE
GROUP BY 1, 2, 3
ORDER BY 1;

-- Give the Account with lastest balance (transaction should be credit otherwise you wont get final latest balance)


SELECT * FROM TRANSACTIONS;

SELECT * FROM TRANSACTIONS LIMIT 10;
-- SELECT * FROM ACCOUNT;
-- SELECT * FROM CLIENT;
-- SELECT * FROM DISTRICT;
-- SELECT COUNT(DISTINCT DISTRICT_ID) FROM ACCOUNT;
-- SELECT COUNT(DISTINCT DISTRICT_CODE) FROM DISTRICT;
