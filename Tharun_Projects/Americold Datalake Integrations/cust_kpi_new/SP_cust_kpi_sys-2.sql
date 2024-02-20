CREATE OR REPLACE PROCEDURE orion_edw.proc_custkpi_sys(psourcesystemcode character varying(50), pfacilityid character varying(50), preportdate date, pglue_job_uid character varying(100))
 LANGUAGE plpgsql
AS $$
/****************************************************************************************
 Author: Martín Esteves
 Creation Date: 2023-11-30
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
BEGIN --Prepare New Log
 vEXEC_ID:=PG_BACKEND_PID();
 vPROC_NAME:= 'PROC_CUSTKPI_SYS';

CALL ORION_EDW.AMERICOLD_JOB_LOG(vPROC_NAME,'OK','Start Proc for GLUE_JOB_ID=' || pGLUE_JOB_UID,null,0,null) ;

--CREATING TEMP TABLE WITH ALL DATA NEEDED FOR THIS FACILITY AS IT WILL BE USED IN EACH QUERY AND GET PALLET CAPACITY
-- GET PALLET_CAPACITY
SELECT PALLET_CAPACITY
into vPALLET_CAPACITY
from orion_edw.facilityInfo
where SOURCE_SYSTEM_CODE = pSourceSystemCode and  FACILITY_ID = pFacilityId;

CREATE TABLE #CUSTOMER_INFO AS
 SELECT COALESCE(sap_customer_id, '-') sap_customer_id, customer_id
            FROM orion_edw.customerinfo
            WHERE source_system_code = pSourceSystemCode
            AND facility_id = pFacilityId;

GET DIAGNOSTICS vROWS_AFFECTED:= ROW_COUNT;
CALL ORION_EDW.AMERICOLD_JOB_LOG(vPROC_NAME,'OK','Create temp table #CUSTOMER_INFO and get Pallet Capacity for GLUE_JOB_ID=' || pGLUE_JOB_UID,'CUSTOMER_INFO',vROWS_AFFECTED,'I') ;


CREATE TABLE #SOURCE_DATA AS
SELECT
    A.report_date,
    A.source_system_code,
    A.facility_id,
    A.customer_id,
    COALESCE(B.sap_customer_id, '-') SAP_CUSTOMER_ID,

    --INV METRICS
    A.current_units,
    A.current_net_weight,
    A.current_gross_weight,
    A.current_volume,
    ROUND(A.current_pallets) current_pallets,
    A.current_pallet_occupancy,
    A.item_count,
    A.whse_lot_count,

    --ORDER METRICS
    A.OUT_UNITS,
    A.OUT_UNIT_PICK_UNITS,
    A.OUT_RECEIPT_CASE_PICK_CALC,
    A.OUT_BILLING_CASE_PICK_CALC,
    A.OUT_NET_WEIGHT,
    A.OUT_GROSS_WEIGHT,
    A.OUT_CATCH_WEIGHT,
    A.OUT_VOLUME,
    A.OUT_TOTAL_ORDERS,
    ROUND(A.OUT_PALLETS) OUT_PALLETS,
    A.OUT_CASEPICK_UNIT_QTY,
    A.OUT_CASEPICK_CATCH_QTY,
    A.OUT_CASEPICK_GROSS_WEIGHT,
    A.OUT_CASEPICK_PALLETS,
    A.OUT_PALPICK_UNIT_QTY,
    A.OUT_PALPICK_CATCH_QTY,
    A.OUT_PALPICK_GROSS_WEIGHT,
    A.OUT_TOTAL_LPNS,
    (A.OUT_PALLETS - A.OUT_CASEPICK_PALLETS) OUT_PALPICK_PALLETS,

    --RECEIPT METRICS
    A.IN_UNITS,
    A.IN_NET_WEIGHT,
    A.IN_GROSS_WEIGHT,
    A.IN_CATCH_WEIGHT,
    A.IN_VOLUME,
    A.IN_TOTAL_RECEIPTS,
    ROUND(A.IN_PALLETS) IN_PALLETS,

    --GENERAL FIELDS
    A.creator_id,
    A.create_date,
    A.last_updater_id,
    A.last_update_date
FROM (
    (
        SELECT
            DATE(ACTUAL_RCPT_SHIP_DATE) report_date,
            source_system_code,
            facility_id,
            COALESCE(customer_id, '-') CUSTOMER_ID,

            --INV METRICS
            SUM(CASE ref_type
                WHEN 'STORAGE' THEN COALESCE(units, 0)
                ELSE 0
            END) CURRENT_UNITS,
            SUM(CASE ref_type
                WHEN 'STORAGE' THEN COALESCE(net_weight, 0)
                ELSE 0
            END) CURRENT_NET_WEIGHT,
            SUM(CASE ref_type
                WHEN 'STORAGE' THEN COALESCE(gross_weight, 0)
                ELSE 0
            END) CURRENT_GROSS_WEIGHT,
            SUM(CASE ref_type
                WHEN 'STORAGE' THEN COALESCE(volume, 0)
                ELSE 0
            END) CURRENT_VOLUME,
            SUM(CASE ref_type
                WHEN 'STORAGE' THEN COALESCE(receipt_pallet_count_calc, 0) + COALESCE(mixed_pallet_count_calc, 0)
                ELSE 0
            END) CURRENT_PALLETS,
            SUM(CASE ref_type
                WHEN 'STORAGE' THEN COALESCE(receipt_occupancy_count_calc, 0) + COALESCE(mixed_occupancy_count_calc, 0)
                ELSE 0
            END) CURRENT_PALLET_OCCUPANCY,
            COUNT(DISTINCT CASE
                WHEN ref_type = 'STORAGE' THEN item_id
                ELSE NULL
            END) ITEM_COUNT,
            COUNT(DISTINCT CASE
                WHEN ref_type = 'STORAGE' THEN whse_lot_number
                ELSE NULL
            END) WHSE_LOT_COUNT,

            --ORDER METRICS
            SUM(CASE REF_TYPE
                WHEN 'ORDER' THEN COALESCE(UNITS, 0)
                ELSE 0
            END) OUT_UNITS,
            SUM(CASE REF_TYPE
                WHEN 'ORDER' THEN CASE
                        WHEN MIXED_PALLET_COUNT_CALC IS NULL THEN 0
                        ELSE COALESCE(UNITS, 0)
                    END
                ELSE 0
            END) OUT_UNIT_PICK_UNITS,
            SUM(CASE REF_TYPE
                WHEN 'ORDER' THEN COALESCE(LEGACY_RECEIPT_CASE_PICK_CALC, 0)
                ELSE 0
            END) OUT_RECEIPT_CASE_PICK_CALC,
            SUM(CASE REF_TYPE
                WHEN 'ORDER' THEN COALESCE(LEGACY_BILLING_CASE_PICK_CALC, 0)
                ELSE 0
            END) OUT_BILLING_CASE_PICK_CALC,
            SUM(CASE REF_TYPE
                WHEN 'ORDER' THEN COALESCE(NET_WEIGHT, 0)
                ELSE 0
            END) OUT_NET_WEIGHT,
            SUM(CASE REF_TYPE
                WHEN 'ORDER' THEN COALESCE(GROSS_WEIGHT, 0)
                ELSE 0
            END) OUT_GROSS_WEIGHT,
            SUM(CASE REF_TYPE
                WHEN 'ORDER' THEN COALESCE(CATCH_WEIGHT, 0)
                ELSE 0
            END) OUT_CATCH_WEIGHT,
            SUM(CASE REF_TYPE
                WHEN 'ORDER' THEN COALESCE(VOLUME, 0)
                ELSE 0
            END) OUT_VOLUME,
            COUNT(DISTINCT CASE
                WHEN REF_TYPE = 'ORDER' THEN HDR_INTERNAL_ID
                ELSE NULL
            END) OUT_TOTAL_ORDERS,
            SUM(CASE REF_TYPE
                WHEN 'ORDER' THEN COALESCE(RECEIPT_PALLET_COUNT_CALC, 0) + COALESCE(MIXED_PALLET_COUNT_CALC, 0)
                ELSE 0
            END) OUT_PALLETS,
            SUM(CASE REF_TYPE
                WHEN 'ORDER' THEN COALESCE(LEGACY_BILLING_CASE_PICK_CALC, 0)
                ELSE 0
            END) OUT_CASEPICK_UNIT_QTY,
            0 OUT_CASEPICK_CATCH_QTY,
            0 OUT_CASEPICK_GROSS_WEIGHT,
            SUM(CASE REF_TYPE
                WHEN 'ORDER' THEN CASE
                        WHEN LEGACY_BILLING_CASE_PICK_CALC != 0 THEN
                            COALESCE(RECEIPT_PALLET_COUNT_CALC, 0) + COALESCE(MIXED_PALLET_COUNT_CALC, 0)
                        ELSE 0
                    END
                ELSE 0
            END) OUT_CASEPICK_PALLETS,
            SUM(CASE REF_TYPE
                WHEN 'ORDER' THEN COALESCE(UNITS, 0) - COALESCE(LEGACY_BILLING_CASE_PICK_CALC, 0)
                ELSE 0
            END) OUT_PALPICK_UNIT_QTY,
            0 OUT_PALPICK_CATCH_QTY,
            0 OUT_PALPICK_GROSS_WEIGHT,
            SUM(CASE REF_TYPE
                WHEN 'ORDER' THEN COALESCE(LPN_COUNT_CALC, 0)
                ELSE 0
            END) OUT_TOTAL_LPNS,

            --RECEIPT METRICS
            SUM(CASE REF_TYPE
            WHEN 'RECEIPT' THEN COALESCE(UNITS, 0)
            ELSE 0
            END) IN_UNITS,
            SUM(CASE REF_TYPE
            WHEN 'RECEIPT' THEN COALESCE(NET_WEIGHT, 0)
            ELSE 0
            END) IN_NET_WEIGHT,
            SUM(CASE REF_TYPE
            WHEN 'RECEIPT' THEN COALESCE(GROSS_WEIGHT, 0)
            ELSE 0
            END) IN_GROSS_WEIGHT,
            SUM(CASE REF_TYPE
            WHEN 'RECEIPT' THEN COALESCE(CATCH_WEIGHT, 0)
            ELSE 0
            END) IN_CATCH_WEIGHT,
            SUM(CASE REF_TYPE
            WHEN 'RECEIPT' THEN COALESCE(VOLUME, 0)
            ELSE 0
            END) IN_VOLUME,
            COUNT(DISTINCT CASE
            WHEN REF_TYPE = 'RECEIPT' THEN HDR_INTERNAL_ID
            ELSE NULL
            END) IN_TOTAL_RECEIPTS,
            SUM(CASE REF_TYPE
            WHEN 'RECEIPT' THEN COALESCE(RECEIPT_PALLET_COUNT_CALC, 0) + COALESCE(MIXED_PALLET_COUNT_CALC, 0)
            ELSE 0
            END) IN_PALLETS,

            --GENERAL FIELDS
            'SYSTEM' creator_id,
            getdate() create_date,
            'SYSTEM' last_updater_id,
            getdate() last_update_date

        FROM orion_edw.inventory_consolidated H
        WHERE H.kpi_exclude_flag IS NULL
            AND H.ref_type IN ('STORAGE','ORDER','RECEIPT') -- Temporary
            AND date(ACTUAL_RCPT_SHIP_DATE) = pReportDate
            AND H.source_system_code = pSourceSystemCode
            AND h.facility_id = pFacilityId
        GROUP BY DATE(ACTUAL_RCPT_SHIP_DATE),
                source_system_code,
                facility_id,
                ROLLUP (customer_id)
    ) A
    JOIN #CUSTOMER_INFO B ON B.customer_id = A.customer_id
                            AND source_system_code = A.source_system_code
                            AND facility_id = A.facility_id
);

GET DIAGNOSTICS vROWS_AFFECTED:= ROW_COUNT;
CALL ORION_EDW.AMERICOLD_JOB_LOG(vPROC_NAME,'OK','Create temp table #SOURCE_DATA for GLUE_JOB_ID=' || pGLUE_JOB_UID,'CUSTOMER_INFO',vROWS_AFFECTED,'I') ;

--MERGE DATA
MERGE INTO ORION_EDW.CUST_KPI_SYS USING #SOURCE_DATA
                SRC ON CUST_KPI_SYS.REPORT_DATE = SRC.REPORT_DATE
                        AND CUST_KPI_SYS.SOURCE_SYSTEM_CODE = SRC.SOURCE_SYSTEM_CODE
                        AND CUST_KPI_SYS.FACILITY_ID = SRC.FACILITY_ID
                        AND CUST_KPI_SYS.CUSTOMER_ID = SRC.CUSTOMER_ID
                        AND CUST_KPI_SYS.SAP_CUSTOMER_ID = SRC.SAP_CUSTOMER_ID
WHEN MATCHED THEN UPDATE SET
        --INV METRICS
        CURRENT_UNITS = SRC.CURRENT_UNITS,
        CURRENT_NET_WEIGHT = SRC.CURRENT_NET_WEIGHT,
        CURRENT_GROSS_WEIGHT = SRC.CURRENT_GROSS_WEIGHT,
        CURRENT_VOLUME = SRC.CURRENT_VOLUME,
        CURRENT_PALLETS = SRC.CURRENT_PALLETS,
        CURRENT_PALLET_OCCUPANCY = SRC.CURRENT_PALLET_OCCUPANCY,
        ITEM_COUNT = SRC.ITEM_COUNT,
        WHSE_LOT_COUNT = SRC.WHSE_LOT_COUNT,
        --ORDER METRICS
        OUT_UNITS = SRC.OUT_UNITS,
        OUT_UNIT_PICK_UNITS = SRC.OUT_UNIT_PICK_UNITS,
        OUT_RECEIPT_CASE_PICK_CALC = SRC.OUT_RECEIPT_CASE_PICK_CALC,
        OUT_BILLING_CASE_PICK_CALC = SRC.OUT_BILLING_CASE_PICK_CALC,
        OUT_NET_WEIGHT = SRC.OUT_NET_WEIGHT,
        OUT_GROSS_WEIGHT = SRC.OUT_GROSS_WEIGHT,
        OUT_CATCH_WEIGHT = SRC.OUT_CATCH_WEIGHT,
        OUT_VOLUME = SRC.OUT_VOLUME,
        OUT_TOTAL_ORDERS = SRC.OUT_TOTAL_ORDERS,
        OUT_PALLETS = SRC.OUT_PALLETS,
        OUT_CASEPICK_UNIT_QTY = SRC.OUT_CASEPICK_UNIT_QTY,
        OUT_CASEPICK_CATCH_QTY = SRC.OUT_CASEPICK_CATCH_QTY,
        OUT_CASEPICK_GROSS_WEIGHT = SRC.OUT_CASEPICK_GROSS_WEIGHT,
        OUT_CASEPICK_PALLETS = SRC.OUT_CASEPICK_PALLETS,
        OUT_PALPICK_UNIT_QTY = SRC.OUT_PALPICK_UNIT_QTY,
        OUT_PALPICK_CATCH_QTY = SRC.OUT_PALPICK_CATCH_QTY,
        OUT_PALPICK_GROSS_WEIGHT = SRC.OUT_PALPICK_GROSS_WEIGHT,
        OUT_PALPICK_PALLETS = SRC.OUT_PALPICK_PALLETS,
        OUT_TOTAL_LPNS = SRC.OUT_TOTAL_LPNS,
        --RECEIPT METRICS
        IN_UNITS = SRC.IN_UNITS,
        IN_NET_WEIGHT = SRC.IN_NET_WEIGHT,
        IN_GROSS_WEIGHT = SRC.IN_GROSS_WEIGHT,
        IN_CATCH_WEIGHT = SRC.IN_CATCH_WEIGHT,
        IN_VOLUME = SRC.IN_VOLUME,
        IN_TOTAL_RECEIPTS = SRC.IN_TOTAL_RECEIPTS,
        IN_PALLETS = SRC.IN_PALLETS,
        --GENERAL FIELDS
        last_updater_id = 'SYSTEM',
        last_update_date = getdate(),
        AU_UPDATED_DATE = getDate()
WHEN NOT MATCHED THEN
    INSERT( report_date,
            source_system_code,
            facility_id,
            customer_id,
            SAP_CUSTOMER_ID,
            --INV METRICS
            current_units,
            current_net_weight,
            current_gross_weight,
            current_volume,
            current_pallets,
            current_pallet_occupancy,
            item_count,
            whse_lot_count,
            --ORDER METRICS
            OUT_UNITS,
            OUT_UNIT_PICK_UNITS,
            OUT_RECEIPT_CASE_PICK_CALC,
            OUT_BILLING_CASE_PICK_CALC,
            OUT_NET_WEIGHT,
            OUT_GROSS_WEIGHT,
            OUT_CATCH_WEIGHT,
            OUT_VOLUME,
            OUT_TOTAL_ORDERS,
            OUT_PALLETS,
            OUT_CASEPICK_UNIT_QTY,
            OUT_CASEPICK_CATCH_QTY,
            OUT_CASEPICK_GROSS_WEIGHT,
            OUT_CASEPICK_PALLETS,
            OUT_PALPICK_UNIT_QTY,
            OUT_PALPICK_CATCH_QTY,
            OUT_PALPICK_GROSS_WEIGHT,
            OUT_TOTAL_LPNS,
            OUT_PALPICK_PALLETS,
            --RECEIPT METRICS
            IN_UNITS,
            IN_NET_WEIGHT,
            IN_GROSS_WEIGHT,
            IN_CATCH_WEIGHT,
            IN_VOLUME,
            IN_TOTAL_RECEIPTS,
            IN_PALLETS,
            --GENERAL FIELDS
            creator_id,
            create_date,
            last_updater_id,
            last_update_date,
            AU_CREATED_DATE,
            AU_UPDATED_DATE)
    VALUES(
        SRC.report_date,
        SRC.source_system_code,
        SRC.facility_id,
        SRC.customer_id,
        SRC.SAP_CUSTOMER_ID,
        --INV METRICS
        SRC.CURRENT_UNITS,
        SRC.CURRENT_NET_WEIGHT,
        SRC.CURRENT_GROSS_WEIGHT,
        SRC.CURRENT_VOLUME,
        SRC.CURRENT_PALLETS,
        SRC.CURRENT_PALLET_OCCUPANCY,
        SRC.ITEM_COUNT,
        SRC.WHSE_LOT_COUNT,
        --ORDER METRICS
        SRC.OUT_UNITS,
        SRC.OUT_UNIT_PICK_UNITS,
        SRC.OUT_RECEIPT_CASE_PICK_CALC,
        SRC.OUT_BILLING_CASE_PICK_CALC,
        SRC.OUT_NET_WEIGHT,
        SRC.OUT_GROSS_WEIGHT,
        SRC.OUT_CATCH_WEIGHT,
        SRC.OUT_VOLUME,
        SRC.OUT_TOTAL_ORDERS,
        SRC.OUT_PALLETS,
        SRC.OUT_CASEPICK_UNIT_QTY,
        SRC.OUT_CASEPICK_CATCH_QTY,
        SRC.OUT_CASEPICK_GROSS_WEIGHT,
        SRC.OUT_CASEPICK_PALLETS,
        SRC.OUT_PALPICK_UNIT_QTY,
        SRC.OUT_PALPICK_CATCH_QTY,
        SRC.OUT_PALPICK_GROSS_WEIGHT,
        SRC.OUT_PALPICK_PALLETS,
        SRC.OUT_TOTAL_LPNS,
        --RECEIPT METRICS
        SRC.IN_UNITS,
        SRC.IN_NET_WEIGHT,
        SRC.IN_GROSS_WEIGHT,
        SRC.IN_CATCH_WEIGHT,
        SRC.IN_VOLUME,
        SRC.IN_TOTAL_RECEIPTS,
        SRC.IN_PALLETS,
        --GENERAL FIELDS
        'SYSTEM',
        getdate(),
        'SYSTEM',
        getdate(),
        getDate(),
        getDate()
    );

GET DIAGNOSTICS vROWS_AFFECTED:= ROW_COUNT;
CALL ORION_EDW.AMERICOLD_JOB_LOG(vPROC_NAME,'OK','Merge records on CUST_KPI_SYS for GLUE_JOB_ID=' || pGLUE_JOB_UID,'CUST_KPI_SYS',vROWS_AFFECTED,'M') ;

--UPDATE PALLET CAPACITY
UPDATE orion_edw.cust_kpi_sys
SET
    CURRENT_PALLET_CAPACITY = vPALLET_CAPACITY,
    last_updater_id = 'SYSTEM',
    last_update_date = getdate(),
    AU_UPDATED_DATE = GETDATE()
WHERE source_system_code = pSourceSystemCode
    AND facility_id = pFacilityId
    AND report_date = pReportDate;

GET DIAGNOSTICS vROWS_AFFECTED:= ROW_COUNT;
CALL ORION_EDW.AMERICOLD_JOB_LOG(vPROC_NAME,'OK','Update pallet capacity on CUST_KPI_SYS for GLUE_JOB_ID=' || pGLUE_JOB_UID,'CUST_KPI_SYS',vROWS_AFFECTED,'U') ;

CALL ORION_EDW.AMERICOLD_JOB_LOG(vPROC_NAME,'OK','End Proc for GLUE_JOB_ID=' || pGLUE_JOB_UID,null,0,null) ;

DROP TABLE IF EXISTS #CUSTOMER_INFO;
DROP TABLE IF EXISTS #SOURCE_DATA;

EXCEPTION
WHEN OTHERS THEN
vMESSAGE:= 'Error on ' || vPROC_NAME || ' for GLUE_JOB_ID=' || pGLUE_JOB_UID || ' => Error: ' || SQLERRM;
CALL ORION_EDW.AMERICOLD_JOB_LOG(vPROC_NAME,'ERROR',vMESSAGE,NULL,0,NULL) ;
END;
$$
