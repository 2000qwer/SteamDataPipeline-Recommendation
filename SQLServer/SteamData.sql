
EXEC sp_configure 'show advanced options', 1;
RECONFIGURE;
EXEC sp_configure 'user connections';

-- Krok 1: Utwórz login na poziomie serwera
CREATE LOGIN MichalMatracz 
WITH PASSWORD = 'Qwer1997@';

USE [SteamDB];  -- Zast¹p TwojaBazaDanych nazw¹ bazy, do której chcesz dodaæ u¿ytkownika
CREATE USER MichalMatracz FOR LOGIN MichalMatracz;

ALTER ROLE db_owner ADD MEMBER MichalMatracz;


SELECT
*  FROM [SteamDB].[dbo].[SteamGamesDescription] where steam_appid = 2016200

select count(steam_appid) from [SteamDB].[dbo].[SteamGamesDescription]
select *   from [SteamDB].[dbo].[SteamGamesDescription] order by steam_appid desc

select * from [dbo].[SteamGames] where appid = 4500;

select * from [dbo].[SteamGames] where appid = 2018620;


select * from [dbo].[SteamUsersGames]

select * from [dbo].[SteamGames] where appid = 730 order by steamid desc;



select * from [dbo].[SteamUsers] 
where steamid =76561197960444711 order by steamid desc;
select * from [dbo].[SteamUsers];

select distinct steamid from [dbo].[SteamUsersGames] wh

select distinct *  from  SteamGamesDescription
SELECT @@SERVERNAME AS ServerName, SERVERPROPERTY('ProcessID') AS SQLServerPID;

SELECT  steam_appid,* FROM SteamGamesDescription0
WHERE steam_appid = 2103040;



--Temp from [SteamDB].[dbo].[SteamGamesDescription];



select SG.* from [dbo].[SteamUsersGames] SG left join SteamUsers SU on SU.steamid = SG.steamid
left join [dbo].[SteamGames] S ON s.appid = sG.appid
left join [SteamDB].[dbo].[SteamGamesDescription] SGD on SGD.steam_appid = S.appid

select distinct * from [dbo].[SteamUsersGames] where steamid =  76561197960435571

select distinct steamid  from [dbo].[SteamUsersGames]



select * from [dbo].[SteamGames] SG right join [dbo].[SteamGamesDescription] SD on SD.Steam_appid = SG.appid


WITH DuplicateRows AS (
    SELECT 
        steam_appid,
        ROW_NUMBER() OVER (PARTITION BY steam_appid ORDER BY steam_appid) AS row_num
    FROM 
        [SteamDB].[dbo].[SteamGamesDescription]
)
DELETE FROM [SteamDB].[dbo].[SteamGamesDescription]
WHERE steam_appid IN (
    SELECT steam_appid 
    FROM DuplicateRows 
    WHERE row_num > 1
);

select  steam_appid from [SteamDB].[dbo].[SteamGamesDescription];
select distinct steam_appid from [SteamDB].[dbo].[SteamGamesDescription];



--(26464 rows affected)

--(26185 rows affected)


--Creating relation between 4/5 tables 


select top 1 * from [dbo].[SteamGamesDescriptionFix];

select top 1 * from [dbo].[SteamUsers];

select top 1 * from [dbo].[SteamUsersGames];

select top 1 * from [dbo].[SteamGames];

SELECT * FROM SteamGames WHERE appid IS NULL;

ALTER TABLE SteamGamesDescriptionFix
ADD CONSTRAINT FK_SteamGames_DescriptionFix
FOREIGN KEY (steam_appid) REFERENCES SteamGames(appid)



--ALTER TABLE SteamGames
--ALTER COLUMN appid INT NOT NULL;




ALTER TABLE SteamGames
ADD CONSTRAINT PK_SteamGames_AppID PRIMARY KEY (appid);

-- Add primary key to the appid column in SteamGames
ALTER TABLE SteamGames
ADD CONSTRAINT PK_SteamGames_AppID PRIMARY KEY (appid);
--Creating stored procedure thats fixing the main table in term of using in sheduler, so SQL Agent (lets say 1 day)




select * into [SteamDB].[dbo].[SteamGamesDescriptionFix] from [SteamDB].[dbo].[SteamGamesDescription]



select developers,* from [SteamDB].[dbo].[SteamGamesDescriptionFix];


DROP TABLE [SteamDB].[dbo].[SteamGamesDescriptionFix];



select price_overview,* from [SteamDB].[dbo].[SteamGamesDescriptionFix];



--fixed
SELECT 
    supported_languages
FROM [SteamDB].[dbo].[SteamGamesDescriptionFix];


--fixed
SELECT                                                                      -- Replace newlines (\n) with spaces
    *
FROM [SteamDB].[dbo].[SteamGamesDescriptionFix];







--price_overview 
begin transaction;


rollback;
commit;


create or alter procedure TransformSteamTablesData as
	
	DROP TABLE [SteamDB].[dbo].[SteamGamesDescriptionFix];


	select * into [SteamDB].[dbo].[SteamGamesDescriptionFix] from [SteamDB].[dbo].[SteamGamesDescription];
	alter table [SteamDB].[dbo].[SteamGamesDescriptionFix] drop column detailed_description;

	alter table [SteamDB].[dbo].[SteamGamesDescriptionFix] drop column mac_requirements;

	alter table [SteamDB].[dbo].[SteamGamesDescriptionFix] drop column linux_requirements;
	
	update [SteamDB].[dbo].[SteamGamesDescriptionFix] set supported_languages =  REPLACE(
        REPLACE(
            REPLACE(
                REPLACE(
                    REPLACE(supported_languages, 'English*', 'English (Full Audio Support)'), -- Replace English with full audio support
                    '*', ''), 
                '<strong>', ''), 
            '</strong>', ''), 
        '<br>', ' ')



	update [SteamDB].[dbo].[SteamGamesDescriptionFix] set pc_requirements = REPLACE(
        REPLACE(
            REPLACE(
                REPLACE(
                    REPLACE(
                        REPLACE(
                            REPLACE(
                                REPLACE(
                                    REPLACE(
                                        REPLACE(
                                            REPLACE(
                                                REPLACE(
                                                    REPLACE(
                                                        REPLACE(pc_requirements, '"minimum":', 'Minimum Requirements:'), -- Replace the JSON key for "minimum"
                                                        '"recommended":', ' Recommended Requirements:'),               -- Replace the JSON key for "recommended"
                                                    '{', ''),                                    -- Remove opening curly brace
                                                '}', ''),                                    -- Remove closing curly brace
                                            '<ul class=\"bb_ul\">', ''),                      -- Specifically target and remove <ul class="bb_ul">
                                        '</ul>', ''),                                    -- Remove closing unordered list tag
                                    '<br>', '\n'),                                       -- Replace <br> with newline character
                                '</li>', ''),                                            -- Remove closing list item tag
                            '<li>', ''),                                                 -- Remove opening list item tag
                        '<strong>', ''),                                                 -- Remove opening strong tag
                    '</strong>', ''),                                                    -- Remove closing strong tag
                '"', ''),                                                                -- Remove extra quotes around content
            '\n<ul class=bb_ul>', ''),                                                   -- Remove escaped <ul> tag pattern
        '\n', ' ')



		UPDATE [SteamDB].[dbo].[SteamGamesDescriptionFix]
		SET pc_requirements = NULL
		WHERE pc_requirements = '[]';
		
		UPDATE [SteamDB].[dbo].[SteamGamesDescriptionFix]
		SET short_description = NULL
		WHERE short_description = '';
		
		
		UPDATE [SteamDB].[dbo].[SteamGamesDescriptionFix]
			SET developers =REPLACE(developers, '[', '')
			WHERE developers IS NOT NULL;
		UPDATE [SteamDB].[dbo].[SteamGamesDescriptionFix]
			SET developers =REPLACE(developers, ']', '')
			WHERE developers IS NOT NULL;


		UPDATE [SteamDB].[dbo].[SteamGamesDescriptionFix]
			SET publishers =REPLACE(developers, '[', '')
			WHERE publishers IS NOT NULL;
		UPDATE [SteamDB].[dbo].[SteamGamesDescriptionFix]
			SET publishers =REPLACE(developers, ']', '')
			WHERE publishers IS NOT NULL;


		UPDATE [SteamDB].[dbo].[SteamGamesDescriptionFix]
			SET price_overview =REPLACE(price_overview, '{}', null)
			WHERE price_overview = '{}';
		
		ALTER TABLE SteamGamesDescriptionFix
			ADD currency NVARCHAR(10),
			initial_price INT,
			final_price INT,
			discount_percent INT,
			initial_formatted NVARCHAR(50),
			final_formatted NVARCHAR(50);

		UPDATE SteamGamesDescriptionFix
		SET currency = JSON_VALUE(price_overview, '$.currency'),
			initial_price = JSON_VALUE(price_overview, '$.initial'),
			final_price = JSON_VALUE(price_overview, '$.final'),
			discount_percent = JSON_VALUE(price_overview, '$.discount_percent'),
			initial_formatted = JSON_VALUE(price_overview, '$.initial_formatted'),
			final_formatted = JSON_VALUE(price_overview, '$.final_formatted')
		WHERE price_overview IS NOT NULL;

		UPDATE SteamGamesDescriptionFix
		SET final_formatted = 
			REPLACE(final_formatted, 'z³', '')
		WHERE final_formatted IS NOT NULL;

		ALTER TABLE SteamGamesDescriptionFix
			ADD [windows] NVARCHAR(10),
			mac NVARCHAR(10),
			linux NVARCHAR(10);

		UPDATE SteamGamesDescriptionFix
		SET [windows] = JSON_VALUE(platforms, '$.windows'),
			mac = JSON_VALUE(platforms, '$.mac'),
			linux = JSON_VALUE(platforms, '$.linux')
		WHERE platforms IS NOT NULL;
		

		alter table [SteamDB].[dbo].[SteamGamesDescriptionFix] drop column platforms;
		alter table [SteamDB].[dbo].[SteamGamesDescriptionFix] drop column price_overview;

		---Relation adding 

		--Steam Games crerating primary key 
		ALTER TABLE SteamGames
		ADD CONSTRAINT PK_SteamGames_AppID PRIMARY KEY (appid);





		--UPDATE SteamGamesDescriptionFix
		--SET metacritic = 
		--	REPLACE(metacritic, '{}', '')
		--WHERE metacritic = '{}';


		
EXEC TransformSteamTablesData;


SELECT 
    jobs.name AS JobName, *
FROM 
    msdb.dbo.sysjobs AS jobs
LEFT JOIN 
    msdb.dbo.sysjobactivity AS activity ON jobs.job_id = activity.job_id
LEFT JOIN 
    msdb.dbo.sysjobhistory AS history ON jobs.job_id = history.job_id
    AND history.instance_id = activity.last_executed_step_id
ORDER BY 
    jobs.name, history.run_date DESC;


select * from [SteamDB].[dbo].[SteamGamesDescriptionFix];
