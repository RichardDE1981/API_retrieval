
/*
MergeDimDate.sql
------------------------------------------------------
Purpose:
- Updates/merges the calendar table for a given year range
- Uses the date dimension function dbo.ufn_GetDates
- Logs automatically if needed
- Parameters:
    @StartYear INT
    @EndYear INT
------------------------------------------------------
*/



CREATE  OR ALTER PROCEDURE [dbo].[usp_merge_Calendar]
(
    @StartYear INT = NULL,
    @EndYear   INT = NULL
)
AS
BEGIN
    SET NOCOUNT ON;



    -- MERGE calendar data from function into target table

    MERGE INTO dbo.CALENDAR AS target
    USING dbo.ufn_GetDates(@StartYear,@EndYear) AS source
    ON target.DateID = source.DateID
    WHEN MATCHED THEN
        UPDATE SET
            Day = source.Day,
            DaySuffix = source.DaySuffix,
            DayName = source.DayName,
            DayOfWeek = source.DayOfWeek,
            DayOfYear = source.DayOfYear,
            Week = source.Week,
            ISOWeek = source.ISOWeek,
            Month = source.Month,
            MonthName = source.MonthName,
            Quarter = source.Quarter,
            Year = source.Year,
            FirstOfMonth = source.FirstOfMonth,
            LastOfMonth = source.LastOfMonth,
            FirstOfNextMonth = source.FirstOfNextMonth,
            LastOfNextMonth = source.LastOfNextMonth,
            FirstOfYear = source.FirstOfYear,
            LastOfYear = source.LastOfYear,
            IsLeapYear = source.IsLeapYear,
            Has53Weeks = source.Has53Weeks,
            Has53ISOWeeks = source.Has53ISOWeeks,
            MMYYYY = source.MMYYYY,
            Style101 = source.Style101,
            Style103 = source.Style103,
            Style112 = source.Style112,
            Style120 = source.Style120,
            IsWeekend = source.IsWeekend,
            FirstOfWeek = source.FirstOfWeek,
            LastOfWeek = source.LastOfWeek,
            atr_month = source.atr_month,
            atr_week = source.atr_week,
            atr_week_ISO = source.atr_week_ISO
    WHEN NOT MATCHED THEN
        INSERT (
            DateID, Day, DaySuffix, DayName, DayOfWeek, DayOfYear,
            Week, ISOWeek, Month, MonthName, Quarter, Year,
            FirstOfMonth, LastOfMonth, FirstOfNextMonth, LastOfNextMonth,
            FirstOfYear, LastOfYear, IsLeapYear, Has53Weeks, Has53ISOWeeks,
            MMYYYY, Style101, Style103, Style112, Style120, IsWeekend,
            FirstOfWeek, LastOfWeek, atr_month, atr_week, atr_week_ISO
        )
        VALUES (
            source.DateID, source.Day, source.DaySuffix, source.DayName, source.DayOfWeek, source.DayOfYear,
            source.Week, source.ISOWeek, source.Month, source.MonthName, source.Quarter, source.Year,
            source.FirstOfMonth, source.LastOfMonth, source.FirstOfNextMonth, source.LastOfNextMonth,
            source.FirstOfYear, source.LastOfYear, source.IsLeapYear, source.Has53Weeks, source.Has53ISOWeeks,
            source.MMYYYY, source.Style101, source.Style103, source.Style112, source.Style120, source.IsWeekend,
            source.FirstOfWeek, source.LastOfWeek, source.atr_month, source.atr_week, source.atr_week_ISO
        );
        END



        *******************



DECLARE @StartYear INT;
DECLARE @EndYear   INT;

SET @StartYear = YEAR(GETDATE()) - 5;
SET @EndYear   = YEAR(GETDATE()) + 1;

EXEC dbo.usp_MergeCalendar
    @StartYear = @StartYear,
    @EndYear   = @EndYear;