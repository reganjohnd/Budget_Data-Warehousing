-- create dates dimension table
CREATE TABLE [Date_DIM](
	[date_key] int NOT NULL,
	[full_date] date NULL,
	[month] char(10) NULL,
   [year] smallint NULL,
    [month_num] smallint NULL,
    [week_num] smallint NULL,
    [week_day] char(10) NULL,
    [week_day_num] smallint NULL,
	PRIMARY KEY ([date_key])
);

DECLARE @start DATE = '2018-01-01'
DECLARE @end DATE = '2100-01-01'
WHILE @start <= @end
BEGIN
insert into [dbo].[Date_DIM]([date_key], [full_date], [month], [year], [month_num], [week_num], [week_day], [week_day_num])
select DATEPART(YY, @start)*10000+DATEPART(mm, @start)*100+DATEPART(dd, @start) as [date_key],
		@start as [full_date],
		SUBSTRING(DATENAME(mm, @start), 1, 3) as [month],
		DATEPART(YY, @start) as [year],
		DATEPART(mm, @start) as [month_num],
		DATEPART(ww, @start) as [weeknum],
		DATENAME(dw, @start) as [week_day],
		DATEPART(dw, @start) as [week_day_num]
	SET @start = DATEADD(dd, 1, @start)
END

--create spending groups table
CREATE TABLE [spending_groups_DIM](
	[spending_group_key] INT IDENTITY(1, 1) NOT NULL,
	[spending_group_name] varchar(30) NOT NULL,
	PRIMARY KEY ([spending_group_key])
);

INSERT INTO [dbo].[spending_groups_DIM]([spending_group_key], [spending_group_name])
VALUES(857, 'Transfer'), (243, 'Day-to-day'), (658, 'Income'), (435, 'Recurring'), (879, 'Exceptions'), (245, 'Invest-save-repay')

--create categories table
CREATE TABLE [categories_DIM](
	[category_key] INT IDENTITY(1, 1) NOT NULL,
	[category_name] VARCHAR(30) NOT NULL,
	PRIMARY KEY ([category_key])
);

--import the file
BULK INSERT [dbo].[categories_DIM]
FROM 'G:\My Drive\DANIELS ECOSYSTEM\extracurricularActivities\DataScience\Projects\budget_warehouse\category_insert.csv'
WITH
(
       FORMAT='CSV',
       FIRSTROW=2
)
GO

create accounts table

CREATE TABLE [accounts_DIM](
	[account_key] INT IDENTITY(1, 1) NOT NULL,
	[account_name] VARCHAR(50) NULL,
	[account_detail] VARCHAR(50) NOT NULL,
	[is_active] TINYINT NULL,
	PRIMARY KEY ([account_key])
);

BULK INSERT [dbo].[accounts_DIM]
FROM 'G:\My Drive\DANIELS ECOSYSTEM\extracurricularActivities\DataScience\Projects\budget_warehouse\accounts_insert.csv'
WITH
(
	FORMAT='CSV',
	FIRSTROW=2
)
GO

CREATE TABLE [transactions_Fact](
	[transaction_key] INT IDENTITY (1, 1) NOT NULL,
	[date_key] INT NOT NULL,
	[category_key] INT NOT NULL,
	[spending_group_key] INT NOT NULL,
	[account_key] INT NOT NULL,
	[description] VARCHAR(500) NOT NULL,
	[amount] DECIMAL (19, 4) NOT NULL,
	PRIMARY KEY([transaction_key])
);

ALTER TABLE date_DIM
ADD [YYYY-MM] VARCHAR(7);

UPDATE [dbo].[date_DIM]
SET [YYYY-MM] = (SUBSTRING(CONVERT(VARCHAR(7), [full_date]), 1, 7));
