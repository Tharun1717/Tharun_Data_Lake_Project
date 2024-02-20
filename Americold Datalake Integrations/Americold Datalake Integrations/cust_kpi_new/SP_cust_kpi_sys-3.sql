CREATE OR REPLACE PROCEDURE orion_edw.proc_custkpi_sys_merge(psourcesystemcode character varying(50), pfacilityid character varying(50), pstartdate date, penddate date, pglue_job_uid character varying(100))
 LANGUAGE plpgsql
AS $$
/****************************************************************************************
 Author: Martín Esteves
 Creation Date: 2023-12-04
 Description:

 Execution Sample:

 *****************************************************************************************/
DECLARE
vEXEC_ID NUMERIC(18, 0);
vPROC_NAME VARCHAR(1000);
vMESSAGE VARCHAR(1000);
vROWS_AFFECTED NUMERIC(18, 0);
vREC NUMERIC(18, 0);
vRUN_DATE_TZ TIMESTAMPTZ;
vPALLET_CAPACITY BIGINT;
vMETRICS_CALCULATED_FOR VARCHAR(1000);
vKPI_FIELDS VARCHAR(1000);

curKPI CURSOR FOR
    select DISTINCT METRICS_CALCULATED_FOR
    from ORION_EDW.CUST_KPI_NEW_MERGE_STAGING;

BEGIN --Prepare New Log
 vEXEC_ID:=PG_BACKEND_PID();
 vPROC_NAME:= 'PROC_CUSTKPI_SYS_MERGE';

CALL ORION_EDW.AMERICOLD_JOB_LOG(vPROC_NAME,'OK','Start Proc for GLUE_JOB_ID=' || pGLUE_JOB_UID,null,0,null) ;

--POPULATE MERGE STAGE TABLE
CALL ORION_EDW.PROC_CUSTKPI_SYS_MERGE_STG(psourcesystemcode,pfacilityid,pstartdate,penddate,pglue_job_uid);

CREATE TABLE #CUSTOMER_INFO AS
 SELECT COALESCE(sap_customer_id, '-') sap_customer_id, customer_id
            FROM orion_edw.customerinfo
            WHERE source_system_code = pSourceSystemCode
            AND facility_id = pFacilityId;

GET DIAGNOSTICS vROWS_AFFECTED:= ROW_COUNT;
CALL ORION_EDW.AMERICOLD_JOB_LOG(vPROC_NAME,'OK','Create temp table #CUSTOMER_INFO and get Pallet Capacity for GLUE_JOB_ID=' || pGLUE_JOB_UID,'CUSTOMER_INFO',vROWS_AFFECTED,'I') ;

OPEN curKPI;
LOOP

    FETCH curKPI into vMETRICS_CALCULATED_FOR;
    EXIT WHEN NOT FOUND;

    IF vMETRICS_CALCULATED_FOR='OUTCUTS' THEN
        CREATE TABLE #OUTCUTS AS
                        SELECT DISTINCT
                            A.REPORT_DATE
                            , A.SOURCE_SYSTEM_CODE
                            , A.FACILITY_ID
                            , A.CUSTOMER_ID
                            , COALESCE(B.sap_customer_id, '-') SAP_CUSTOMER_ID
                            , A.OUT_CUT_UNITS
                            , A.CREATOR_ID
                            , A.CREATE_DATE
                            , A.LAST_UPDATER_ID
                            , A.LAST_UPDATE_DATE
                        FROM
                            (
                                (
                                    SELECT DISTINCT
                                    c.REPORT_DATE
                                    , c.SOURCE_SYSTEM_CODE
                                    , c.FACILITY_ID
                                    , c.CUSTOMER_ID
                                    , 'SYSTEM' creator_id
                                    , getdate() create_date
                                    , 'SYSTEM' last_updater_id
                                    , getdate() last_update_date
                                    , SUM(C.OUT_CUT_UNITS) OUT_CUT_UNITS
                            FROM orion_edw.cust_kpi_new_merge_staging c
                                    WHERE c.source_system_code = psourcesystemcode
                                    AND c.facility_id = pfacilityid
                                    AND c.METRICS_CALCULATED_FOR = 'OUTCUTS'
                                GROUP BY report_date, source_system_code, facility_id, customer_id
                                ) A
                                JOIN #CUSTOMER_INFO B ON B.customer_id = A.customer_id
                                                            AND source_system_code = A.source_system_code
                                                            AND facility_id = A.facility_id
                            );

        GET DIAGNOSTICS vROWS_AFFECTED:= ROW_COUNT;
        CALL ORION_EDW.AMERICOLD_JOB_LOG(vPROC_NAME,'OK','Insert on #OUTCUTS table for GLUE_JOB_ID=' || pGLUE_JOB_UID,'#OUTCUTS',vROWS_AFFECTED,'I');


        MERGE INTO ORION_EDW.CUST_KPI_SYS USING #OUTCUTS
                SRC ON CUST_KPI_SYS.REPORT_DATE = SRC.REPORT_DATE
                        AND CUST_KPI_SYS.SOURCE_SYSTEM_CODE = SRC.SOURCE_SYSTEM_CODE
                        AND CUST_KPI_SYS.FACILITY_ID = SRC.FACILITY_ID
                        AND CUST_KPI_SYS.CUSTOMER_ID = SRC.CUSTOMER_ID
                        AND CUST_KPI_SYS.SAP_CUSTOMER_ID = SRC.SAP_CUSTOMER_ID
                WHEN MATCHED THEN UPDATE SET
                    OUT_CUT_UNITS = SRC.OUT_CUT_UNITS,
                    last_updater_id = 'SYSTEM',
                    last_update_date = getdate(),
                    AU_UPDATED_DATE = GETDATE()
                WHEN NOT MATCHED THEN
                    INSERT(REPORT_DATE,SOURCE_SYSTEM_CODE,FACILITY_ID,CUSTOMER_ID,SAP_CUSTOMER_ID,OUT_CUT_UNITS,CREATOR_ID,CREATE_DATE,LAST_UPDATER_ID,LAST_UPDATE_DATE,AU_CREATED_DATE,AU_UPDATED_DATE)
                    VALUES(SRC.REPORT_DATE,SRC.SOURCE_SYSTEM_CODE,SRC.FACILITY_ID,SRC.CUSTOMER_ID,SRC.SAP_CUSTOMER_ID,
                            SRC.OUT_CUT_UNITS,
                            SRC.CREATOR_ID,SRC.CREATE_DATE,SRC.LAST_UPDATER_ID,SRC.LAST_UPDATE_DATE,GETDATE(),GETDATE());

        DROP TABLE IF EXISTS #OUTCUTS;

        GET DIAGNOSTICS vROWS_AFFECTED:= ROW_COUNT;
        CALL ORION_EDW.AMERICOLD_JOB_LOG(vPROC_NAME,'OK','Merge OUTCUTS in CUST_KPI_SYS table for GLUE_JOB_ID=' || pGLUE_JOB_UID,'CUST_KPI_SYS',vROWS_AFFECTED,'M');
    END IF;

    IF vMETRICS_CALCULATED_FOR='AVGLEADTIME' THEN

        CREATE TABLE #AVGLEADTIME  AS
                        SELECT DISTINCT
                            A.REPORT_DATE
                            , A.SOURCE_SYSTEM_CODE
                            , A.FACILITY_ID
                            , A.CUSTOMER_ID
                            , COALESCE(B.sap_customer_id, '-') SAP_CUSTOMER_ID
                            , A.AVG_LEAD_TIME
                            , A.CREATOR_ID
                            , A.CREATE_DATE
                            , A.LAST_UPDATER_ID
                            , A.LAST_UPDATE_DATE
                        FROM
                            (
                                (
                                    SELECT DISTINCT
                                    c.REPORT_DATE
                                    , c.SOURCE_SYSTEM_CODE
                                    , c.FACILITY_ID
                                    , c.CUSTOMER_ID
                                    , 'SYSTEM' creator_id
                                    , getdate() create_date
                                    , 'SYSTEM' last_updater_id
                                    , getdate() last_update_date
                                    , SUM(C.avg_lead_time) AVG_LEAD_TIME
                            FROM orion_edw.cust_kpi_new_merge_staging c
                                    WHERE c.source_system_code = psourcesystemcode
                                    AND c.facility_id = pfacilityid
                                    AND c.METRICS_CALCULATED_FOR = 'AVGLEADTIME'
                                GROUP BY report_date, source_system_code, facility_id, customer_id
                                ) A
                                JOIN #CUSTOMER_INFO B ON B.customer_id = A.customer_id
                                                            AND source_system_code = A.source_system_code
                                                            AND facility_id = A.facility_id
                            );

        GET DIAGNOSTICS vROWS_AFFECTED:= ROW_COUNT;
        CALL ORION_EDW.AMERICOLD_JOB_LOG(vPROC_NAME,'OK','Insert on #AVGLEADTIME table for GLUE_JOB_ID=' || pGLUE_JOB_UID,'#AVGLEADTIME',vROWS_AFFECTED,'I');

        MERGE INTO ORION_EDW.CUST_KPI_SYS USING #AVGLEADTIME SRC
                    ON CUST_KPI_SYS.REPORT_DATE = SRC.REPORT_DATE
                        AND CUST_KPI_SYS.SOURCE_SYSTEM_CODE = SRC.SOURCE_SYSTEM_CODE
                        AND CUST_KPI_SYS.FACILITY_ID = SRC.FACILITY_ID
                        AND CUST_KPI_SYS.CUSTOMER_ID = SRC.CUSTOMER_ID
                        AND CUST_KPI_SYS.SAP_CUSTOMER_ID = SRC.SAP_CUSTOMER_ID
                WHEN MATCHED THEN UPDATE SET
                    AVG_LEAD_TIME = SRC.AVG_LEAD_TIME,
                    last_updater_id = 'SYSTEM',
                    last_update_date = getdate(),
                    AU_UPDATED_DATE = GETDATE()
                WHEN NOT MATCHED THEN
                    INSERT(REPORT_DATE,SOURCE_SYSTEM_CODE,FACILITY_ID,CUSTOMER_ID,SAP_CUSTOMER_ID,AVG_LEAD_TIME,CREATOR_ID,CREATE_DATE,LAST_UPDATER_ID,LAST_UPDATE_DATE,AU_CREATED_DATE,AU_UPDATED_DATE)
                    VALUES(SRC.REPORT_DATE,SRC.SOURCE_SYSTEM_CODE,SRC.FACILITY_ID,SRC.CUSTOMER_ID,SRC.SAP_CUSTOMER_ID,
                            SRC.AVG_LEAD_TIME,
                            SRC.CREATOR_ID,SRC.CREATE_DATE,SRC.LAST_UPDATER_ID,SRC.LAST_UPDATE_DATE,GETDATE(),GETDATE());

        GET DIAGNOSTICS vROWS_AFFECTED:= ROW_COUNT;
        CALL ORION_EDW.AMERICOLD_JOB_LOG(vPROC_NAME,'OK','Merge #AVGLEADTIME in CUST_KPI_SYS table for GLUE_JOB_ID=' || pGLUE_JOB_UID,'CUST_KPI_SYS',vROWS_AFFECTED,'M');

        DROP TABLE IF EXISTS #AVGLEADTIME;

    END IF;

    IF vMETRICS_CALCULATED_FOR='TOTALADJUSTMENTS' THEN

        CREATE TABLE #TOTALADJUSTMENTS AS
                        SELECT
                        A.REPORT_DATE
                        , A.SOURCE_SYSTEM_CODE
                        , A.FACILITY_ID
                        , COALESCE(A.CUSTOMER_ID,'-') CUSTOMER_ID
                        , COALESCE(B.sap_customer_id, '-') SAP_CUSTOMER_ID
                        , A.total_adjusted_units_plus
                        , A.total_adjusted_units_minus
                        , A.TOTAL_ADJUSTED_UNITS
                        , A.creator_id
                        , A.create_date
                        , A.last_updater_id
                        , A.last_update_date
                        FROM
                        (
                            (
                                SELECT DISTINCT
                                    c.REPORT_DATE
                                    , c.SOURCE_SYSTEM_CODE
                                    , c.FACILITY_ID
                                    , coalesce(c.CUSTOMER_ID,'-') CUSTOMER_ID
                                    , sum(C.total_adjusted_units_plus) total_adjusted_units_plus
                                    , sum(c.total_adjusted_units_minus) total_adjusted_units_minus
                                    , sum(ABS(c.total_adjusted_units_plus)) + sum(ABS(C.total_adjusted_units_minus)) TOTAL_ADJUSTED_UNITS
                                    , 'SYSTEM' creator_id
                                    , getdate() create_date
                                    , 'SYSTEM' last_updater_id
                                    , getdate() last_update_date
                                FROM orion_edw.cust_kpi_new_merge_staging c
                                WHERE c.source_system_code = psourcesystemcode
                                        AND c.facility_id = pfacilityid
                                        AND c.METRICS_CALCULATED_FOR = 'TOTALADJUSTMENTS'
                                group by REPORT_DATE, SOURCE_SYSTEM_CODE, FACILITY_ID, rollup(CUSTOMER_ID)
                            ) A
                                JOIN #CUSTOMER_INFO B ON B.customer_id = A.customer_id
                                                            AND source_system_code = A.source_system_code
                                                            AND facility_id = A.facility_id
                        );

        GET DIAGNOSTICS vROWS_AFFECTED:= ROW_COUNT;
        CALL ORION_EDW.AMERICOLD_JOB_LOG(vPROC_NAME,'OK','Insert on #TOTALADJUSTMENTS table for GLUE_JOB_ID=' || pGLUE_JOB_UID,'#TOTALADJUSTMENTS',vROWS_AFFECTED,'I');

        MERGE INTO ORION_EDW.CUST_KPI_SYS USING #TOTALADJUSTMENTS SRC
                ON CUST_KPI_SYS.REPORT_DATE = SRC.REPORT_DATE
                        AND CUST_KPI_SYS.SOURCE_SYSTEM_CODE = SRC.SOURCE_SYSTEM_CODE
                        AND CUST_KPI_SYS.FACILITY_ID = SRC.FACILITY_ID
                        AND CUST_KPI_SYS.CUSTOMER_ID = SRC.CUSTOMER_ID
                        AND CUST_KPI_SYS.SAP_CUSTOMER_ID = SRC.SAP_CUSTOMER_ID
                WHEN MATCHED THEN UPDATE SET
                    total_adjusted_units_plus = SRC.total_adjusted_units_plus,
                    total_adjusted_units_minus = SRC.total_adjusted_units_minus,
                    TOTAL_ADJUSTED_UNITS = SRC.TOTAL_ADJUSTED_UNITS,
                    last_updater_id = 'SYSTEM',
                    last_update_date = getdate(),
                    AU_UPDATED_DATE = GETDATE()
                WHEN NOT MATCHED THEN
                    INSERT(REPORT_DATE,SOURCE_SYSTEM_CODE,FACILITY_ID,CUSTOMER_ID,SAP_CUSTOMER_ID,total_adjusted_units_plus,total_adjusted_units_minus,
                            TOTAL_ADJUSTED_UNITS,CREATOR_ID,CREATE_DATE,LAST_UPDATER_ID,LAST_UPDATE_DATE,AU_CREATED_DATE,AU_UPDATED_DATE)
                    VALUES(SRC.REPORT_DATE,SRC.SOURCE_SYSTEM_CODE,SRC.FACILITY_ID,SRC.CUSTOMER_ID,SRC.SAP_CUSTOMER_ID,
                            SRC.total_adjusted_units_plus,SRC.total_adjusted_units_minus,SRC.TOTAL_ADJUSTED_UNITS,
                            SRC.CREATOR_ID,SRC.CREATE_DATE,SRC.LAST_UPDATER_ID,SRC.LAST_UPDATE_DATE,GETDATE(),GETDATE());

        GET DIAGNOSTICS vROWS_AFFECTED:= ROW_COUNT;
        CALL ORION_EDW.AMERICOLD_JOB_LOG(vPROC_NAME,'OK','Merge #TOTALADJUSTMENTS in CUST_KPI_SYS table for GLUE_JOB_ID=' || pGLUE_JOB_UID,'CUST_KPI_SYS',vROWS_AFFECTED,'M');

        DROP TABLE IF EXISTS #TOTALADJUSTMENTS;

    END IF;

    IF vMETRICS_CALCULATED_FOR='AMCOUTCUTS' THEN

        CREATE TABLE #AMCOUTCUTS AS
                        SELECT
                        A.REPORT_DATE
                        , A.SOURCE_SYSTEM_CODE
                        , A.FACILITY_ID
                        , COALESCE(A.CUSTOMER_ID,'-') CUSTOMER_ID
                        , COALESCE(B.sap_customer_id, '-') SAP_CUSTOMER_ID
                        , A.TOTAL_AMC_OUT_CUT_UNITS
                        , A.creator_id
                        , A.create_date
                        , A.last_updater_id
                        , A.last_update_date
                        FROM
                        (
                            (
                                SELECT DISTINCT
                                    c.REPORT_DATE
                                    , c.SOURCE_SYSTEM_CODE
                                    , c.FACILITY_ID
                                    , coalesce(c.CUSTOMER_ID,'-') CUSTOMER_ID
                                    , SUM(C.TOTAL_AMC_OUT_CUT_UNITS) TOTAL_AMC_OUT_CUT_UNITS
                                    , 'SYSTEM' creator_id
                                    , getdate() create_date
                                    , 'SYSTEM' last_updater_id
                                    , getdate() last_update_date
                                FROM orion_edw.cust_kpi_new_merge_staging c
                                WHERE c.source_system_code = psourcesystemcode
                                        AND c.facility_id = pfacilityid
                                        AND c.METRICS_CALCULATED_FOR = 'AMCOUTCUTS'
                                group by REPORT_DATE, SOURCE_SYSTEM_CODE, FACILITY_ID, rollup(CUSTOMER_ID)
                            ) A
                                JOIN #CUSTOMER_INFO B ON B.customer_id = A.customer_id
                                                            AND source_system_code = A.source_system_code
                                                            AND facility_id = A.facility_id
                        );

        GET DIAGNOSTICS vROWS_AFFECTED:= ROW_COUNT;
        CALL ORION_EDW.AMERICOLD_JOB_LOG(vPROC_NAME,'OK','Insert on #AMCOUTCUTS table for GLUE_JOB_ID=' || pGLUE_JOB_UID,'#AMCOUTCUTS',vROWS_AFFECTED,'I');

        MERGE INTO ORION_EDW.CUST_KPI_SYS USING #AMCOUTCUTS SRC
                ON CUST_KPI_SYS.REPORT_DATE = SRC.REPORT_DATE
                        AND CUST_KPI_SYS.SOURCE_SYSTEM_CODE = SRC.SOURCE_SYSTEM_CODE
                        AND CUST_KPI_SYS.FACILITY_ID = SRC.FACILITY_ID
                        AND CUST_KPI_SYS.CUSTOMER_ID = SRC.CUSTOMER_ID
                        AND CUST_KPI_SYS.SAP_CUSTOMER_ID = SRC.SAP_CUSTOMER_ID
                WHEN MATCHED THEN UPDATE SET
                    TOTAL_AMC_OUT_CUT_UNITS = SRC.TOTAL_AMC_OUT_CUT_UNITS,
                    last_updater_id = 'SYSTEM',
                    last_update_date = getdate(),
                    AU_UPDATED_DATE = GETDATE()
                WHEN NOT MATCHED THEN
                    INSERT(REPORT_DATE,SOURCE_SYSTEM_CODE,FACILITY_ID,CUSTOMER_ID,SAP_CUSTOMER_ID,TOTAL_AMC_OUT_CUT_UNITS,CREATOR_ID,CREATE_DATE,LAST_UPDATER_ID,LAST_UPDATE_DATE,AU_CREATED_DATE,AU_UPDATED_DATE)
                    VALUES(SRC.REPORT_DATE,SRC.SOURCE_SYSTEM_CODE,SRC.FACILITY_ID,SRC.CUSTOMER_ID,SRC.SAP_CUSTOMER_ID,
                            SRC.TOTAL_AMC_OUT_CUT_UNITS,
                            SRC.CREATOR_ID,SRC.CREATE_DATE,SRC.LAST_UPDATER_ID,SRC.LAST_UPDATE_DATE,GETDATE(),GETDATE());

        GET DIAGNOSTICS vROWS_AFFECTED:= ROW_COUNT;
        CALL ORION_EDW.AMERICOLD_JOB_LOG(vPROC_NAME,'OK','Merge #AMCOUTCUTS in CUST_KPI_SYS table for GLUE_JOB_ID=' || pGLUE_JOB_UID,'CUST_KPI_SYS',vROWS_AFFECTED,'M');

        DROP TABLE IF EXISTS #AMCOUTCUTS;

    END IF;

    IF vMETRICS_CALCULATED_FOR='TOTALAMCADJUSTMENTS' THEN

        CREATE TABLE #TOTALAMCADJUSTMENTS AS
                    SELECT
                        A.REPORT_DATE
                        , A.SOURCE_SYSTEM_CODE
                        , A.FACILITY_ID
                        , COALESCE(A.CUSTOMER_ID,'-') CUSTOMER_ID
                        , COALESCE(B.sap_customer_id, '-') SAP_CUSTOMER_ID
                        , A.AMC_ADJUSTED_UNITS_PLUS
                        , A.AMC_ADJUSTED_UNITS_MINUS
                        , A.TOTAL_AMC_ADJUSTED_UNITS
                        , A.creator_id
                        , A.create_date
                        , A.last_updater_id
                        , A.last_update_date
                        FROM
                        (
                            (
                                SELECT DISTINCT
                                    c.REPORT_DATE
                                    , c.SOURCE_SYSTEM_CODE
                                    , c.FACILITY_ID
                                    , coalesce(c.CUSTOMER_ID,'-') CUSTOMER_ID
                                    , sum(C.AMC_ADJUSTED_UNITS_PLUS) AMC_ADJUSTED_UNITS_PLUS
                                    , sum(c.AMC_ADJUSTED_UNITS_MINUS) AMC_ADJUSTED_UNITS_MINUS
                                    , sum(ABS(c.AMC_ADJUSTED_UNITS_MINUS)) + sum(ABS(C.AMC_ADJUSTED_UNITS_PLUS)) TOTAL_AMC_ADJUSTED_UNITS
                                    , 'SYSTEM' creator_id
                                    , getdate() create_date
                                    , 'SYSTEM' last_updater_id
                                    , getdate() last_update_date
                                FROM orion_edw.cust_kpi_new_merge_staging c
                                WHERE c.source_system_code = psourcesystemcode
                                        AND c.facility_id = pfacilityid
                                        AND c.METRICS_CALCULATED_FOR = 'TOTALAMCADJUSTMENTS'
                                group by REPORT_DATE, SOURCE_SYSTEM_CODE, FACILITY_ID, rollup(CUSTOMER_ID)
                            ) A
                                JOIN #CUSTOMER_INFO B ON B.customer_id = A.customer_id
                                                            AND source_system_code = A.source_system_code
                                                            AND facility_id = A.facility_id
                        );

        GET DIAGNOSTICS vROWS_AFFECTED:= ROW_COUNT;
        CALL ORION_EDW.AMERICOLD_JOB_LOG(vPROC_NAME,'OK','Insert on #TOTALAMCADJUSTMENTS table for GLUE_JOB_ID=' || pGLUE_JOB_UID,'#TOTALAMCADJUSTMENTS',vROWS_AFFECTED,'I');

        MERGE INTO ORION_EDW.CUST_KPI_SYS USING #TOTALAMCADJUSTMENTS SRC
                ON CUST_KPI_SYS.REPORT_DATE = SRC.REPORT_DATE
                        AND CUST_KPI_SYS.SOURCE_SYSTEM_CODE = SRC.SOURCE_SYSTEM_CODE
                        AND CUST_KPI_SYS.FACILITY_ID = SRC.FACILITY_ID
                        AND CUST_KPI_SYS.CUSTOMER_ID = SRC.CUSTOMER_ID
                        AND CUST_KPI_SYS.SAP_CUSTOMER_ID = SRC.SAP_CUSTOMER_ID
                WHEN MATCHED THEN UPDATE SET
                    AMC_ADJUSTED_UNITS_PLUS = SRC.AMC_ADJUSTED_UNITS_PLUS,
                    AMC_ADJUSTED_UNITS_MINUS = SRC.AMC_ADJUSTED_UNITS_MINUS,
                    TOTAL_AMC_ADJUSTED_UNITS = SRC.TOTAL_AMC_ADJUSTED_UNITS,
                    last_updater_id = 'SYSTEM',
                    last_update_date = getdate(),
                    AU_UPDATED_DATE = GETDATE()
                WHEN NOT MATCHED THEN
                    INSERT(REPORT_DATE,SOURCE_SYSTEM_CODE,FACILITY_ID,CUSTOMER_ID,SAP_CUSTOMER_ID,
                            AMC_ADJUSTED_UNITS_PLUS,AMC_ADJUSTED_UNITS_MINUS,TOTAL_AMC_ADJUSTED_UNITS,
                            CREATOR_ID,CREATE_DATE,LAST_UPDATER_ID,LAST_UPDATE_DATE,AU_CREATED_DATE,AU_UPDATED_DATE)
                    VALUES(SRC.REPORT_DATE,SRC.SOURCE_SYSTEM_CODE,SRC.FACILITY_ID,SRC.CUSTOMER_ID,SRC.SAP_CUSTOMER_ID,
                            SRC.AMC_ADJUSTED_UNITS_PLUS,SRC.AMC_ADJUSTED_UNITS_MINUS,SRC.TOTAL_AMC_ADJUSTED_UNITS,
                            SRC.CREATOR_ID,SRC.CREATE_DATE,SRC.LAST_UPDATER_ID,SRC.LAST_UPDATE_DATE,GETDATE(),GETDATE());

        GET DIAGNOSTICS vROWS_AFFECTED:= ROW_COUNT;
        CALL ORION_EDW.AMERICOLD_JOB_LOG(vPROC_NAME,'OK','Merge #TOTALAMCADJUSTMENTS in CUST_KPI_SYS table for GLUE_JOB_ID=' || pGLUE_JOB_UID,'CUST_KPI_SYS',vROWS_AFFECTED,'M');

        DROP TABLE IF EXISTS #TOTALAMCADJUSTMENTS;
    END IF;

END LOOP;

CLOSE curKPI;

DROP TABLE IF EXISTS #CUSTOMER_INFO;

CALL ORION_EDW.AMERICOLD_JOB_LOG(vPROC_NAME,'OK','End Proc for GLUE_JOB_ID=' || pGLUE_JOB_UID,null,0,null) ;


EXCEPTION
WHEN OTHERS THEN
vMESSAGE:= 'Error on ' || vPROC_NAME || ' for GLUE_JOB_ID=' || pGLUE_JOB_UID || ' => Error: ' || SQLERRM;
CALL ORION_EDW.AMERICOLD_JOB_LOG(vPROC_NAME,'ERROR',vMESSAGE,NULL,0,NULL) ;
END;
$$
