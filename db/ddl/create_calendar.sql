/*
calendar_table.sql
------------------------------------------------------
Purpose:
- Updates/merges the calendar table for a given year range
- Uses the calendar function dbo.ufn_GetDateDimension
- Logs automatically if needed
- Parameters:
    @StartYear INT
    @EndYear INT
------------------------------------------------------
*/



IF NOT EXISTS (SELECT 1 FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA='dbo' AND TABLE_NAME='CALENDAR')
BEGIN

CREATE TABLE [dbo].[CALENDAR](
	[DateID] [date] NOT NULL,
	[Day] [int] NULL,
	[DaySuffix] [char](2) NULL,
	[DayName] [nvarchar](20) NULL,
	[DayOfWeek] [int] NULL,
	[DayOfYear] [int] NULL,
	[Week] [int] NULL,
	[ISOWeek] [int] NULL,
	[Month] [int] NULL,
	[MonthName] [nvarchar](20) NULL,
	[Quarter] [int] NULL,
	[Year] [int] NULL,
	[FirstOfMonth] [date] NULL,
	[LastOfMonth] [date] NULL,
	[FirstOfNextMonth] [date] NULL,
	[LastOfNextMonth] [date] NULL,
	[FirstOfYear] [date] NULL,
	[LastOfYear] [date] NULL,
	[IsLeapYear] [bit] NULL,
	[Has53Weeks] [bit] NULL,
	[Has53ISOWeeks] [bit] NULL,
	[MMYYYY] [char](6) NULL,
	[Style101] [char](10) NULL,
	[Style103] [char](10) NULL,
	[Style112] [char](8) NULL,
	[Style120] [char](10) NULL,
	[IsWeekend] [bit] NULL,
	[FirstOfWeek] [date] NULL,
	[LastOfWeek] [date] NULL,
	[atr_month] [char](7) NULL,
	[atr_week] [nvarchar](15) NULL,
	[atr_week_ISO] [nvarchar](15) NULL,
PRIMARY KEY CLUSTERED
(
	[DateID] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON, OPTIMIZE_FOR_SEQUENTIAL_KEY = OFF) ON [PRIMARY]
)

END
