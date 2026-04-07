Create Database if not exists bankdb;

Use bankdb;

Set SQL_safe_updates=0;

SELECT * FROM bankdb.`bank loan data cleaned`;

------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
# KPIs
# 1. TOTAL LOAN AMOUNT DISBURSED

Select SUM(`Loan Amount`) as Total_Loan_Disbursed
from bankdb.`bank loan data cleaned`;

------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
# 2. AVERAGE INTEREST RATE

Select Round(AVG(`Int Rate`),4) as Avg_Interest_Rate
from bankdb.`bank loan data cleaned`;

------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
# 3. AVERAGE LOAN TENURE (IN MONTHS)

Select AVG(CAST(REPLACE(Term,'months','') as unsigned)) as Avg_Term_Months
from bankdb.`bank loan data cleaned`;

------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
# 4. DEFAULT RATE

Select 
    (SUM(CASE when `Is Default Loan` = 'Y' then 1 else 0 END) 
     / COUNT(*)) * 100 as Default_Rate_Percent
from bankdb.`bank loan data cleaned`;

------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
# 5. DELINQUENCY RATE (DELINQ 2 YEARS)

Select
	(SUM(CASE when `Delinq 2 Yrs` > 0 then 1 else 0 END)
    / COUNT(*)) * 100 as Delinquency_Rate
from bankdb.`bank loan data cleaned`;

------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
# 6. COLLECTION EFFICIENCY

Select
   ROUND(
      (SUM(`Total Rec Prncp`) / SUM(`Loan Amount`)) * 100, 2) as Collection_Efficiency
from bankdb.`bank loan data cleaned`;

------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
# 7. RECOVERY RATE

Select
   ROUND((SUM(`Recoveries`) / SUM(`Loan Amount`))*100, 2) as Recovery_Rate
from bankdb.`bank loan data cleaned`;

------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
# 8. BRANCH-WISE LOAN DISBURSEMENT

Select
    `Branch Name`,
    SUM(`Loan Amount`) as Total_Loan
from bankdb.`bank loan data cleaned`
Group by `Branch Name`
Order by Total_Loan DESC;  

------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
# 9. CASTE-WISE LOAN SUMMARY

Update bankdb.`bank loan data cleaned`
   set Caste = 'Unknown'                           #  For updating
   where Caste IS NULL OR TRIM(Caste) = '';

DELETE FROM bankdb.`bank loan data cleaned`        # For deleting
WHERE Caste IS NULL OR TRIM(Caste) = '';       

Select
    Caste,
    COUNT(*) as Total_Customers,
    SUM(`Loan Amount`) as Total_Loan
from bankdb.`bank loan data cleaned`   
Group by Caste
Order by Total_Loan DESC;   
   
------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ 
# 10. PURPOSE CATEGORY-WISE LOAN DISTRIBUTION

Select 
   `Purpose Category`,
   COUNT(*) as Total_Loans,
   SUM(`Loan Amount`) as Total_Loan_Amount
from bankdb.`bank loan data cleaned`
Group by `Purpose Category`
Order by Total_Loan_Amount DESC;

------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
# INSIGHTs
# 1. HIGH-RISK SEGMENT

Select 
    Age,
    SUM(`Is Default Loan` = 'Y') as Defaults
from bankdb.`bank loan data cleaned`
Group by Age
Order by Defaults DESC;

------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
# 2. BRANCHES WITH LOW COLLECTION EFFICIENCY

Select 
    `Branch Name`,
    ROUND((SUM(`Total Rec Prncp`) / SUM(`Loan Amount`))*100,2) as Collection_Efficiency
from bankdb.`bank loan data cleaned`
Group by  `Branch Name`
Order by Collection_Efficiency ASC;

------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
# 3. INTEREST RATE IMPACT

Select 
    CASE 
        when `Int Rate` < 0.1 then 'Low Interest'
        when `Int Rate` between 0.1 AND 0.15 then 'Medium Interest'
        else 'High Interest'
    END as Interest_Bucket,
    COUNT(*) as Total_Loans,
    SUM(`Is Default Loan` = 'Y') as Default_Count
from bankdb.`bank loan data cleaned`
GROUP BY Interest_Bucket;

------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
# 4. BRANCH WITH THE HIGHEST LOAN DISBURSEMENT

Select `Branch Name`, SUM(`Loan Amount`) as Total_Loan
from `bankdb`.`bank loan data cleaned`
Group by `Branch Name`
Order by Total_Loan DESC
Limit 1;

------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
# 5. TOP 5 CUSTOMERS WITH HIGHEST INTEREST RATES

Select `Account ID`, `Client Name`, `Loan Amount`, `Int Rate`
from `bankdb`.`bank loan data cleaned`
Order by `Int Rate` DESC
Limit 5;

------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
# 6. TOP 10 BRANCH BY RECOVERIES

Select `Branch Name`, SUM(`Recoveries`) as Total_Recoveries
from `bankdb`.`bank loan data cleaned`
Group by `Branch Name`
Order by Total_Recoveries DESC
Limit 10;

------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
# TRIGGERS
# 1. BEFORE INSERT: VALIDATE LOAN AMOUNT

DELIMITER $$
Create Trigger trg_before_insert_loan
BEFORE INSERT ON `bankdb`.`bank loan data cleaned`
FOR EACH ROW
BEGIN
    If new.`Loan Amount` <= 0 then
        Signal SQLState '45000'
        set Message_Text = 'Loan Amount must be positive.';
    END If;
END$$

DELIMITER ;

Insert into `bank loan data cleaned` (`Loan Amount`)
values (-100);                                                #Loan Amount must be positive. 

Insert into `bank loan data cleaned` (`Loan Amount`)
values (50000);

------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
# 2. AFTER UPDATE: LOG DEFAULT LOANS

# CREATE LOG TABLE

Create table Loan_Default_Log (
    Log_ID int auto_increment primary key,
    Account_ID varchar(50),
    Default_Flag varchar(10),
    Updated_At Timestamp Default Current_Timestamp
);

# CREATE TRIGGER

DELIMITER $$
Create Trigger trg_after_update_default
AFTER UPDATE ON `bankdb`.`bank loan data cleaned`
FOR EACH ROW
BEGIN
    If new.`Is Default Loan` = 'Y' AND old.`Is Default Loan` <> 'Y' then
        insert into `loan_default_log` (Account_ID, Default_Flag)
        values (new.`Account ID`, new.`Is Default Loan`);
    END If;
END$$

DELIMITER ;

Update `bank loan data cleaned`
Set `Is Default Loan` = 'Y'
Where `Account ID` = '0010XLG01';

Select * from Loan_Default_Log;

------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
# 3. AUTO-CALCULATE COLLECTION EFFICIENCY ON UPDATE

# ADD COLUMN COLLECTION EFFICIENCY

Alter Table bankdb.`bank loan data cleaned`
add column collection_efficiency DECIMAL(10,2) default null;

# TRIGGER

DELIMITER $$
Create Trigger trg_update_collection_efficiency
BEFORE UPDATE ON `bankdb`.`bank loan data cleaned`
FOR EACH ROW
BEGIN
    IF new.`Loan Amount` > 0 then
        set new.collection_efficiency = 
            ROUND((new.`Total Rec Prncp` / new.`Loan Amount`) * 100, 2);
    END If;
END$$

DELIMITER ;

Update `bank loan data cleaned`
Set `Total Rec Prncp` = 50000
Where `Account ID` = '0010XLG01'; 

Select `Account ID`, `Loan Amount`, `Total Rec Prncp`, collection_efficiency
from `bank loan data cleaned`
Where `Account ID` = '0010XLG01';

------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
# 4. TRACK INTEREST RATE CHANGES

# CREATE LOG TABLE

Create Table Interest_Rate_Log (
    Log_ID int auto_increment primary key,
    Account_ID varchar(50),
    Old_Rate decimal(10,5),
    New_Rate decimal(10,5),
    Changed_At Timestamp Default Current_Timestamp
);

# TRIGGER

DELIMITER $$

Create Trigger trg_interest_rate_change
AFTER UPDATE ON bankdb.`bank loan data cleaned`
FOR EACH ROW
BEGIN
    If old.`Int Rate` <> new.`Int Rate` then
        insert into interest_rate_log (account_id, old_rate, new_rate)
        values (new.`Account ID`, old.`Int Rate`, new.`Int Rate`);
    END If;
END$$

DELIMITER ;

Update `bank loan data cleaned`
Set `Int Rate` = 12.75000
Where `Account ID` = 'ACC1005';

Select * from Interest_Rate_Log;

------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
# 5. PREVENT INSERT IF AGE GROUP IS INVALID

DELIMITER $$

Create Trigger trg_validate_age_group
BEFORE INSERT ON `bankdb`.`bank loan data cleaned`
FOR EACH ROW
BEGIN
    If new.`Age` NOT IN ('18-25','26-35','36-45','46-55','56-65') then
        Signal SQLState '45000'
        set Message_Text = 'Invalid Age Group!';
    END If;
END$$

DELIMITER ;

INSERT INTO `bankdb`.`bank loan data cleaned` (
    `Account ID`, `Age`, `Loan Amount`
) VALUES (                                                 # Invalid Entry
    'TEST001', '70-80', 50000
);

INSERT INTO `bankdb`.`bank loan data cleaned` (
    `Account ID`, `Age`, `Loan Amount`
) VALUES (                                                 # Valid Entry
    'TEST002', '26-35', 60000
);


-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------
# 6. LOAN OVERDUE WARNING LOG

DROP TABLE IF EXISTS overdue_log;

# CREATE LOG TABLE

Create Table Overdue_Log (
  ID int auto_increment primary key,
  Account_ID varchar(50),
  Delinq_Value int,
  Logged_At Timestamp Default Current_Timestamp
);

# TRIGGER

DELIMITER $$

Create Trigger trg_overdue_log
AFTER UPDATE ON `bankdb`.`bank loan data cleaned`
FOR EACH ROW
BEGIN
    IF new.`Delinq 2 Yrs` > 0 AND old.`Delinq 2 Yrs` = 0 then
        insert into`Overdue_Log`(Account_ID, Delinq_Value)
        values (new.`Account ID`, new.`Delinq 2 Yrs`);
    END If;
END$$

DELIMITER ;

Update `bank loan data cleaned`
Set `Delinq 2 Yrs` = 3
Where `Account ID` = '0010XLG01';

Select * from Overdue_Log;

------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
# VIEWs
# 1. LOAN SUMMARY

Create View Loan_Summary as
Select
  COUNT(*) as Total_Loans,
  SUM(`Loan Amount`) as Total_Loan_Amount,
  ROUND(AVG(`Int Rate`),4) as Avg_Interest_Rate,
  ROUND(SUM(`Is Default Loan` = 'Y')/COUNT(*)*100,2) as Default_Rate
from `bankdb`.`bank loan data cleaned`;

Select * from bankdb.loan_summary;

------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
# 2. BRANCH PERFORMANCE

Create View Branch_Performance as
Select
  `Branch Name`,
  COUNT(*) as Loans,
  SUM(`Loan Amount`) as disbursement,
  ROUND(AVG(`Int Rate`),4) as Avg_Rate
from `bankdb`.`bank loan data cleaned`
Group by `Branch Name`;

Select * from bankdb.branch_performance;

------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
# STORED PROCEDURES 
# 1. BRANCH REPORT

DELIMITER $$

Create Procedure Branch_Report(IN branchname VARCHAR(100))
BEGIN
  Select * from Branch_Performance
  where `Branch Name` = branchname;

  Select * from `bankdb`.`bank loan data cleaned`
  where `Branch Name` = branchname
  Order by `Loan Amount` DESC;
END $$

DELIMITER ;

call bankdb.Branch_Report('PATIALA');

------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
# FUNCTIONs 
# 1. CALCULATE EMI FROM LOAN AMOUNT + INTEREST RATE + TENURE

DELIMITER $$

Create Function calc_emi(loan DECIMAL(10,2), rate DECIMAL(10,5), months INT)
RETURNS DECIMAL(10,2)
DETERMINISTIC
BEGIN
    DECLARE r DECIMAL(10,5);
    DECLARE emi DECIMAL(10,2);

    set r = rate / 12;

    set emi = (loan * r * POW(1+r, months)) / (POW(1+r, months) - 1);

    RETURN ROUND(emi, 2);
END $$

DELIMITER ;

SELECT Calc_EMI(100000, 0.12, 24);

------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
# 2. BUCKET CUSTOMERS BY RISK

DROP FUNCTION IF EXISTS risk_bucket;

DELIMITER $$

Create Function Risk_Bucket(Default_Flag varchar(10), Delinq int, Int_Rate DECIMAL(10,5))
RETURNS varchar(20)
DETERMINISTIC
BEGIN
    If Default_Flag = 'Yes' then
        RETURN 'High Risk';
    ElseIf Delinq > 0 then
        RETURN 'Medium Risk';
    ElseIf Int_Rate > 0.15 then
        RETURN 'Medium Risk';
    Else 
        RETURN 'Low Risk';
    END If;
END $$

DELIMITER ;

Select Risk_Bucket('Yes', 0, 0.12);                #   Returns: High Risk

SELECT Risk_Bucket('No', 2, 0.12);                 #   Returns: Medium Risk

SELECT Risk_Bucket('No', 0, 0.16);                 #   Returns: Medium Risk

SELECT Risk_Bucket('No', 0, 0.12);                 #   Returns: Low Risk

------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
# 3. COLLECTION EFFICIENCY %

Drop Function Collection_Efficiency;

DELIMITER $$

Create Function Collection_Efficiency(Paid DECIMAL(10,2), Loan DECIMAL(10,2))
RETURNS DECIMAL(10,2)
DETERMINISTIC
BEGIN
    If loan = 0 then RETURN 0; 
    END If;
    RETURN ROUND((Paid / Loan) * 100, 2);
END $$

DELIMITER ;

Select Collection_Efficiency(`Total Rec Prncp`, `Loan Amount`)
from `bankdb`.`bank loan data cleaned`;

Select Collection_Efficiency(5000, 10000) as Collection_Efficiency;                      #  Returns: 50.00

Select Collection_Efficiency(12000, 12000) as Collection_Efficiency;                     #  Returns: 100.00   

------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
------------------------------------------------------------------------------------------------------------------------------------------------------------------------------