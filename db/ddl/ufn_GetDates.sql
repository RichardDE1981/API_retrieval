/* =========================================================================================
   Function Name: dbo.ufn_GetDates
   Description:
       Inline table-valued function that generates a complete date dimension
       between @StartYear and @EndYear (inclusive).

       The function dynamically creates a continuous set of dates using a 
       tally (numbers) table approach built from CTE cross joins.

       It returns a basic calendar dataset

       The function is fully set-based and does NOT rely on loops.

   Parameters:
       @StartYear  INT   → First year to generate (e.g., 2000)
       @EndYear    INT   → Last year to generate (e.g., 2030)

   Returns:
       One row per calendar date including:
           - Standard date attributes
           - ISO week attributes
           - Month/Year boundaries
           - Week boundaries
           - Leap year indicator
           - 53-week indicators
           - Formatted string representations
           - Weekend flag (DATEFIRST aware)

   Notes:
       - Week calculations depend on @@DATEFIRST.
       - ISO week follows ISO-8601 standard.
       - Designed for SQL Server 2012+ (uses DATEFROMPARTS, EOMONTH, FORMAT).

   Example:
       SELECT *
       FROM dbo.ufn_GetDates(2020, 2025);

========================================================================================= */

IF OBJECT_ID('dbo.ufn_GetDates', 'IF') IS NOT NULL
    DROP FUNCTION dbo.ufn_GetDates;
GO

CREATE FUNCTION [dbo].[ufn_GetDates]
(
    @StartYear INT,
    @EndYear   INT
)
RETURNS TABLE
AS
RETURN

/* =========================================================================================
   Step 1: Generate a Tally (Numbers) Table
   -----------------------------------------------------------------------------------------
   We generate a sufficiently large rowset using cross joins:
       E1  → 10 rows
       E2  → 100 rows
       E4  → 10,000 rows

   This is enough to generate ~27 years of dates.
   Increase cross joins if a wider range is required.
========================================================================================= */

WITH
E1(N) AS (
    SELECT 1 UNION ALL SELECT 1 UNION ALL SELECT 1 UNION ALL SELECT 1 UNION ALL SELECT 1
    UNION ALL SELECT 1 UNION ALL SELECT 1 UNION ALL SELECT 1 UNION ALL SELECT 1 UNION ALL SELECT 1
),
E2(N) AS (SELECT 1 FROM E1 a CROSS JOIN E1 b),
E4(N) AS (SELECT 1 FROM E2 a CROSS JOIN E2 b),

Tally(N) AS (
    SELECT ROW_NUMBER() OVER (ORDER BY (SELECT NULL)) - 1
    FROM E4
),

/* =========================================================================================
   Step 2: Generate Continuous Dates
   -----------------------------------------------------------------------------------------
   Start from January 1st of @StartYear and add N days.
   Stop once we exceed December 31st of @EndYear.
========================================================================================= */

Dates AS (
    SELECT DATEADD(DAY, N, DATEFROMPARTS(@StartYear, 1, 1)) AS [Date]
    FROM Tally
    WHERE DATEADD(DAY, N, DATEFROMPARTS(@StartYear, 1, 1))
          <= DATEFROMPARTS(@EndYear, 12, 31)
)

/* =========================================================================================
   Step 3: Enrich Each Date With Calendar Attributes
========================================================================================= */

SELECT
    /* Core Date */
    d.[Date] AS DateID,

    /* Basic Day Attributes */
    DAY(d.[Date]) AS [Day],

    /* Day suffix (st, nd, rd, th) */
    CAST(
        CASE
            WHEN DAY(d.[Date]) / 10 = 1 THEN 'th'  /* 11th, 12th, 13th */
            ELSE RIGHT('stndrdth', 2 * CASE RIGHT(DAY(d.[Date]), 1)
                WHEN '1' THEN 1
                WHEN '2' THEN 2
                WHEN '3' THEN 3
                ELSE 4 END)
        END AS NVARCHAR(2)
    ) AS DaySuffix,

    CAST(DATENAME(WEEKDAY, d.[Date]) AS NVARCHAR(30)) AS DayName,
    DATEPART(WEEKDAY, d.[Date]) AS DayOfWeek,
    DATEPART(DAYOFYEAR, d.[Date]) AS DayOfYear,

    /* Week Information */
    DATEPART(WEEK, d.[Date]) AS Week,
    DATEPART(ISO_WEEK, d.[Date]) AS ISOWeek,

    /* Month / Quarter / Year */
    DATEPART(MONTH, d.[Date]) AS [Month],
    CAST(DATENAME(MONTH, d.[Date]) AS NVARCHAR(30)) AS MonthName,
    DATEPART(QUARTER, d.[Date]) AS Quarter,
    DATEPART(YEAR, d.[Date]) AS [Year],

    /* Month Boundaries */
    DATEFROMPARTS(YEAR(d.[Date]), MONTH(d.[Date]), 1) AS FirstOfMonth,
    EOMONTH(d.[Date]) AS LastOfMonth,
    DATEADD(MONTH, 1, DATEFROMPARTS(YEAR(d.[Date]), MONTH(d.[Date]), 1)) AS FirstOfNextMonth,
    DATEADD(DAY, -1,
        DATEADD(MONTH, 2, DATEFROMPARTS(YEAR(d.[Date]), MONTH(d.[Date]), 1))
    ) AS LastOfNextMonth,

    /* Year Boundaries */
    DATEFROMPARTS(YEAR(d.[Date]), 1, 1) AS FirstOfYear,
    DATEFROMPARTS(YEAR(d.[Date]), 12, 31) AS LastOfYear,

    /* Leap Year Indicator */
    CASE 
        WHEN (YEAR(d.[Date]) % 400 = 0)
          OR (YEAR(d.[Date]) % 4 = 0 AND YEAR(d.[Date]) % 100 <> 0)
        THEN 1 ELSE 0
    END AS IsLeapYear,

    /* 53 Week Indicators */
    CASE 
        WHEN DATEPART(WEEK, DATEFROMPARTS(YEAR(d.[Date]), 12, 31)) = 53
        THEN 1 ELSE 0
    END AS Has53Weeks,

    CASE 
        WHEN DATEPART(ISO_WEEK, DATEFROMPARTS(YEAR(d.[Date]), 12, 31)) = 53
        THEN 1 ELSE 0
    END AS Has53ISOWeeks,

    /* Common Date Formats */
    CAST(FORMAT(d.[Date], 'MMyyyy') AS NVARCHAR(6)) AS MMYYYY,
    CAST(CONVERT(CHAR(10), d.[Date], 101) AS NVARCHAR(10)) AS Style101,
    CAST(CONVERT(CHAR(10), d.[Date], 103) AS NVARCHAR(10)) AS Style103,
    CAST(CONVERT(CHAR(8),  d.[Date], 112) AS NVARCHAR(8))  AS Style112,
    CAST(CONVERT(CHAR(10), d.[Date], 120) AS NVARCHAR(10)) AS Style120,

    /* Weekend Flag (respects @@DATEFIRST setting) */
    CASE 
        WHEN DATEPART(WEEKDAY, d.[Date]) IN 
            (CASE @@DATEFIRST WHEN 1 THEN 6 WHEN 7 THEN 1 END, 7)
        THEN 1 ELSE 0
    END AS IsWeekend,

    /* Week Boundaries */
    DATEADD(DAY, 1 - DATEPART(WEEKDAY, d.[Date]), d.[Date]) AS FirstOfWeek,
    DATEADD(DAY, 7 - DATEPART(WEEKDAY, d.[Date]), d.[Date]) AS LastOfWeek,

    /* Analytical Attributes (Useful for BI) */
    CAST(FORMAT(d.[Date], 'yyyy-MM') AS NVARCHAR(7)) AS atr_month,

    CAST(
        FORMAT(d.[Date], 'yyyy-MM') 
        + '-W' 
        + RIGHT('0' + CAST(DATEPART(WEEK, d.[Date]) AS VARCHAR(2)), 2)
        AS NVARCHAR(20)
    ) AS atr_week,

    CAST(
        CAST(YEAR(d.[Date]) AS VARCHAR(4))
        + '-IW'
        + RIGHT('0' + CAST(DATEPART(ISO_WEEK, d.[Date]) AS VARCHAR(2)), 2)
        AS NVARCHAR(20)
    ) AS atr_week_ISO

FROM Dates d;
GO