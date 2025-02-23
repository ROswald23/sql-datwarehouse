/*
Creation de database and schemas
ce sript crée une base de données et verifie si elle existe deja. 
si elle existe le script l'efface et la recrée.
le script ajoute ensuite 3 schemas : bronze, silver, gold

ATTENTION
le script efface toute la base done ne pas executer entierement
*/


USE master;
GO

-- EFFACER ET RECREER LA BASE Datawarehouse
IF EXISTS (SELECT 1 FROM sys.databases WHERE name = 'Datawarehouse')
BEGIN
	ALTER DATABASE Datawarehouse SET SINGLE_USER WITH ROLLBACK IMMIDIATE;
	DROP DATABASE Datawarehouse;
END;
GO

-- RECREER LA BASE Datawarehouse
CREATE DATABASE Datawarehouse;
GO
-- utilser ma base
USE Datawarehouse; 
GO
--crée les schema pour chaque etapes
CREATE SCHEMA bronze;
GO

CREATE SCHEMA silver;
GO

CREATE SCHEMA gold;
GO
