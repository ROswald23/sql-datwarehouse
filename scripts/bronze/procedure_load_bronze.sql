--CREATION DE STORED PROCEDURE POUR SAVE REQUETES REPETITIVES

CREATE OR ALTER PROCEDURE bronze.load_bronze AS
BEGIN
	DECLARE @start_time DATETIME, @end_time DATETIME, @batch_start_time DATETIME, @batch_end_time DATETIME;
	-- gestion des erreurs
	BEGIN TRY
		SET @batch_start_time = GETDATE();
	
		-- MESSAGE FOR PROCEDURE
		PRINT '======================================================================';
		PRINT 'Loading Bronze Layer';
		PRINT '======================================================================';

		PRINT '----------------------------------------------------------------------';
		PRINT 'Loading CRM Tables';
		PRINT '----------------------------------------------------------------------';

		SET @start_time = GETDATE();
		PRINT '>> Truncating Table : bronze.crm_cust_info';
		-- pour eviter de charger les données 2 fois dans la meme table en la vidant
		TRUNCATE TABLE bronze.crm_cust_info;
		-- INTEGRATION DES DONNEES DANS LA TABLE RESPECTIVE
		PRINT '>> Inserting Data Into : bronze.crm_cust_info';
		BULK INSERT bronze.crm_cust_info
		FROM 'C:\Users\oswal\OneDrive\Desktop\sql-data-warehouse-project\datasets\source_crm\cust_info.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',' ,
			TABLOCK 
		);
		SET @end_time = GETDATE();
		PRINT '>> Load duration : ' + CAST( DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + 'seconds';
		PRINT '>> ----------------';

		SET @start_time = GETDATE();
		PRINT '>> Truncating Table : bronze.crm_prd_info';
		TRUNCATE TABLE bronze.crm_prd_info;
		PRINT '>> Inserting Data Into : bronze.crm_prd_info';
		BULK INSERT bronze.crm_prd_info
		FROM 'C:\Users\oswal\OneDrive\Desktop\sql-data-warehouse-project\datasets\source_crm\prd_info.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',' ,
			TABLOCK 
		);
		SET @end_time = GETDATE();
		PRINT '>> Load duration : ' + CAST( DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + 'seconds';
		PRINT '>> ----------------';

		SET @start_time = GETDATE();
		PRINT '>> Truncating Table : bronze.crm_sales_details';
		TRUNCATE TABLE bronze.crm_sales_details;
		PRINT '>> Inserting Data Into : bronze.crm_sales_details';
		BULK INSERT bronze.crm_sales_details
		FROM 'C:\Users\oswal\OneDrive\Desktop\sql-data-warehouse-project\datasets\source_crm\sales_details.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',' ,
			TABLOCK 
		);
		SET @end_time = GETDATE();
		PRINT '>> Load duration : ' + CAST( DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + 'seconds';
		PRINT '>> ----------------';
		PRINT '----------------------------------------------------------------------';
		PRINT 'Loading ERP Tables';
		PRINT '----------------------------------------------------------------------';

		SET @start_time = GETDATE();
		PRINT '>> Truncating Table : bronze.erp_cust_az12';
		TRUNCATE TABLE bronze.erp_cust_az12;
		PRINT '>> Inserting Data Into : bronze.erp_cust_az12';
		BULK INSERT bronze.erp_cust_az12
		FROM 'C:\Users\oswal\OneDrive\Desktop\sql-data-warehouse-project\datasets\source_erp\CUST_AZ12.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',' ,
			TABLOCK 
		);
		SET @end_time = GETDATE();
		PRINT '>> Load duration : ' + CAST( DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + 'seconds';
		PRINT '>> ----------------';

		SET @start_time = GETDATE();
		PRINT '>> Truncating Table : bronze.erp_loc_a101';
		TRUNCATE TABLE bronze.erp_loc_a101;
		PRINT '>> Inserting Data Into : bronze.erp_loc_a101';
		BULK INSERT bronze.erp_loc_a101
		FROM 'C:\Users\oswal\OneDrive\Desktop\sql-data-warehouse-project\datasets\source_erp\LOC_A101.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',' ,
			TABLOCK 
		);
		SET @end_time = GETDATE();
		PRINT '>> Load duration : ' + CAST( DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + 'seconds';
		PRINT '>> ----------------';

		SET @start_time = GETDATE();
		PRINT '>> Truncating Table : bronze.erp_px_cat_g1v2';
		TRUNCATE TABLE bronze.erp_px_cat_g1v2;
		PRINT '>> Inserting Data Into : bronze.erp_px_cat_g1v2';
		BULK INSERT bronze.erp_px_cat_g1v2
		FROM 'C:\Users\oswal\OneDrive\Desktop\sql-data-warehouse-project\datasets\source_erp\PX_CAT_G1V2.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',' ,
			TABLOCK 
		);
		SET @end_time = GETDATE();
		PRINT '>> Load duration : ' + CAST( DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + 'seconds';
		PRINT '>> ----------------';
		-- TESTER LA QUALITE DE LA TABLE DE RECHERCHE
		SELECT * FROM bronze.crm_cust_info
		-- verifier le nombre de ligne
		SELECT COUNT(*) AS nb_total_de_lignes_du_dtaframe FROM bronze.crm_cust_info 


		SELECT * FROM bronze.crm_prd_info
		SELECT COUNT(*) AS nb_total_de_lignes_du_dtaframe FROM bronze.crm_prd_info

		SELECT * FROM bronze.crm_sales_details
		SELECT COUNT(*) AS nb_total_de_lignes_du_dtaframe FROM bronze.crm_sales_details

		SELECT * FROM bronze.erp_cust_az12
		SELECT COUNT(*) AS nb_total_de_lignes_du_dtaframe FROM bronze.erp_cust_az12

		SELECT * FROM bronze.erp_loc_a101
		SELECT COUNT(*) AS TOTAL FROM bronze.erp_loc_a101

		SELECT * FROM bronze.erp_px_cat_g1v2
		SELECT COUNT(*) as dernier_total FROM bronze.erp_px_cat_g1v2
		SET @batch_end_time = GETDATE();
		PRINT '>> Batch Load duration : ' + CAST( DATEDIFF(second, @batch_start_time, @batch_end_time) AS NVARCHAR) + 'seconds';
		PRINT '>> ----------------';
	END TRY
	BEGIN CATCH
		PRINT '======================================================================';
		PRINT 'ERROR OCCURED DURING LOADING BRONZE LAYER';
		PRINT 'Error message' + ERROR_MESSAGE();
		PRINT 'Error message' + CAST (ERROR_NUMBER() AS NVARCHAR);
		PRINT '======================================================================';

	END CATCH
END
GO

EXEC bronze.load_bronze
