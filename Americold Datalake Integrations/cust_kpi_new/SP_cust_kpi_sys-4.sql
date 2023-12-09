CREATE OR REPLACE PROCEDURE orion_edw.proc_custkpi_sys_merge_stg(psourcesystemcode character varying(50), pfacilityid character varying(50), pstartdate date, penddate date, pglue_job_uid character varying(100))
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
 vPROC_NAME:= 'PROC_CUSTKPI_SYS_MERGE_STG';

CALL ORION_EDW.AMERICOLD_JOB_LOG(vPROC_NAME,'OK','Start Proc for GLUE_JOB_ID=' || pGLUE_JOB_UID,null,0,null) ;

--DELETE EXISTING DATA TO AVOID DUPLICATIONS
DELETE orion_edw.cust_kpi_new_merge_staging
WHERE SOURCE_SYSTEM_CODE=psourcesystemcode
        AND FACILITY_ID=pfacilityid
        AND REPORT_DATE>=pStartDate AND REPORT_DATE<=pEndDate;
GET DIAGNOSTICS vROWS_AFFECTED:= ROW_COUNT;
CALL ORION_EDW.AMERICOLD_JOB_LOG(vPROC_NAME,'OK','Delete existing data in table CUST_KPI_NEW_MERGE_STAGING for GLUE_JOB_ID=' || pGLUE_JOB_UID,'CUST_KPI_NEW_MERGE_STAGING',vROWS_AFFECTED,'D') ;

--OUTCUTS
    INSERT INTO orion_edw.cust_kpi_new_merge_staging (REPORT_DATE, SOURCE_SYSTEM_CODE, FACILITY_ID, CUSTOMER_ID, OUT_CUT_UNITS, METRICS_CALCULATED_FOR)
        SELECT date(REPORT_DATE) REPORT_DATE,
        SOURCE_SYSTEM_CODE,
        FACILITY_ID,
        coalesce(CUSTOMER_ID, '-') CUSTOMER_ID,
        SUM(OUT_CUT_UNITS) OUT_CUT_UNITS,
        'OUTCUTS' AS METRICS_CALCULATED_FOR
        From
        (
            SELECT distinct date(i.ACTUAL_RCPT_SHIP_DATE) as report_date,
            i.source_system_code,
            i.facility_id,
            coalesce(i.customer_id, '-') customer_id,
            o.hdr_internal_id,
            o.internal_id,
            o.out_cut_units
            FROM
            (
                SELECT ACTUAL_RCPT_SHIP_DATE,
                source_system_code,
                customer_id,
                hdr_internal_id,
                facility_id,
                REF_TYPE
                FROM orion_edw.inventory_consolidated
                WHERE facility_id = pfacilityid
                AND source_system_code = psourcesystemcode
                AND REF_TYPE = 'ORDER'
                AND date(ACTUAL_RCPT_SHIP_DATE) >= date(pstartdate)
                AND date(ACTUAL_RCPT_SHIP_DATE) <= date(penddate)
            ) i
            JOIN
            (
                SELECT Distinct tx.source_system_code,
                tx.document_type,
                tx.facility_id,
                dtl.hdr_internal_id,
                dtl.internal_id,
                tx.day_of_ship_rcv,
                AVG(nvl(dtl.cut_units,0)) OVER(PARTITION BY dtl.internal_id) out_cut_units
                FROM
                orion_edw.document_hdr tx
                JOIN orion_edw.document_dtl dtl
                ON tx.source_system_code = dtl.source_system_code
                AND tx.facility_id = dtl.facility_id
                AND tx.internal_id = dtl.hdr_internal_id
                AND tx.document_type = dtl.document_type
                WHERE tx.facility_id = pfacilityid
                AND tx.source_system_code = psourcesystemcode
                AND tx.day_of_ship_rcv >= DATE(pstartdate)
                AND tx.day_of_ship_rcv <= DATE(penddate)
                AND tx.document_type = 'ORDER'
                AND dtl.cut_units > 0
            ) o
            ON o.source_system_code = i.source_system_code
            AND i.facility_id = o.facility_id
            AND i.hdr_internal_id = o.hdr_internal_id
            WHERE  i.facility_id = pfacilityid
            AND i.source_system_code = psourcesystemcode
            AND date(i.ACTUAL_RCPT_SHIP_DATE) >= date(pstartdate)
            AND date(i.ACTUAL_RCPT_SHIP_DATE) <= date(penddate)
            AND i.REF_TYPE = 'ORDER'
        )
        GROUP BY
        REPORT_DATE,
        SOURCE_SYSTEM_CODE,
        FACILITY_ID,
        CUSTOMER_ID;

GET DIAGNOSTICS vROWS_AFFECTED:= ROW_COUNT;
CALL ORION_EDW.AMERICOLD_JOB_LOG(vPROC_NAME,'OK','Insert OUTCUTS INTO table CUST_KPI_NEW_MERGE_STAGING for GLUE_JOB_ID=' || pGLUE_JOB_UID,'CUST_KPI_NEW_MERGE_STAGING',vROWS_AFFECTED,'I');


--AVGLEADTIME
    INSERT INTO orion_edw.cust_kpi_new_merge_staging (REPORT_DATE, SOURCE_SYSTEM_CODE, FACILITY_ID, CUSTOMER_ID, AVG_LEAD_TIME, METRICS_CALCULATED_FOR)
        SELECT date(REPORT_DATE) REPORT_DATE,
        SOURCE_SYSTEM_CODE,
        FACILITY_ID,
        coalesce(CUSTOMER_ID, '-') CUSTOMER_ID,
        AVG_LEAD_TIME,
        'AVGLEADTIME' AS METRICS_CALCULATED_FOR
        From
        (
            SELECT distinct date(i.ACTUAL_RCPT_SHIP_DATE) as report_date,
            i.source_system_code,
            i.facility_id,
            coalesce(i.customer_id, '-') customer_id,
            avg(o.avgleadtime) avg_lead_time
            FROM
            (
                SELECT ACTUAL_RCPT_SHIP_DATE,
                source_system_code,
                customer_id,
                hdr_internal_id,
                facility_id,
                REF_TYPE
                FROM orion_edw.inventory_consolidated
                WHERE facility_id = pfacilityid
                AND source_system_code = psourcesystemcode
                AND REF_TYPE = 'ORDER'
                AND date(ACTUAL_RCPT_SHIP_DATE) >= date(pstartdate)
                AND date(ACTUAL_RCPT_SHIP_DATE) <= date(penddate)
            ) i
            JOIN
            (
                SELECT DISTINCT
                tx.source_system_code,
                tx.facility_id,
                tx.internal_id,
                tx.day_of_ship_rcv,
                AVG(CAST(EXTRACT(DAY FROM(tx.actual_rcpt_ship_date - tx.create_date) ) + EXTRACT(HOUR FROM(tx.actual_rcpt_ship_date - tx.create_date) ) / 24 AS NUMERIC )) avgleadtime
                FROM orion_edw.document_hdr tx
                WHERE tx.facility_id = pfacilityid
                AND tx.source_system_code = psourcesystemcode
                AND tx.day_of_ship_rcv >= date(pstartdate)
                AND tx.day_of_ship_rcv <= date(penddate)
                AND tx.document_type = 'ORDER'
                AND tx.status = 'DISPATCHED'
                GROUP BY
                tx.source_system_code,
                tx.facility_id,
                tx.internal_id,
                tx.day_of_ship_rcv
            ) o
            ON i.source_system_code = o.source_system_code
            AND i.facility_id = o.facility_id
            AND i.hdr_internal_id = o.internal_id
            WHERE i.facility_id = pfacilityid
            AND i.source_system_code = psourcesystemcode
            AND date(i.ACTUAL_RCPT_SHIP_DATE) >= date(pstartdate)
            AND date(i.ACTUAL_RCPT_SHIP_DATE) <= date(penddate)
            AND i.REF_TYPE = 'ORDER'
            GROUP BY report_date, i.SOURCE_SYSTEM_CODE, i.FACILITY_ID, i.CUSTOMER_ID, o.avgleadtime
        );
GET DIAGNOSTICS vROWS_AFFECTED:= ROW_COUNT;
CALL ORION_EDW.AMERICOLD_JOB_LOG(vPROC_NAME,'OK','Insert AVGLEADTIME INTO table CUST_KPI_NEW_MERGE_STAGING for GLUE_JOB_ID=' || pGLUE_JOB_UID,'CUST_KPI_NEW_MERGE_STAGING',vROWS_AFFECTED,'I');


--TOTALADJUSTMENTS
    INSERT INTO orion_edw.cust_kpi_new_merge_staging (REPORT_DATE, SOURCE_SYSTEM_CODE, FACILITY_ID, CUSTOMER_ID, total_adjusted_units_plus,total_adjusted_units_minus, METRICS_CALCULATED_FOR)
        SELECT date(REPORT_DATE) REPORT_DATE
        , source_system_code
        , facility_id
        , coalesce(customer_id, '-') customer_id
        , coalesce(total_adjusted_units_plus, 0) total_adjusted_units_plus
        , coalesce(total_adjusted_units_minus, 0) total_adjusted_units_minus
        , METRICS_CALCULATED_FOR
        from
        (
            SELECT date(o.transaction_date) as report_date,
            i.source_system_code,
            i.facility_id,
            coalesce(i.customer_id, '-') customer_id,
            SUM(CASE WHEN o.ref_type = 'ADJUSTMENT IN' THEN COALESCE(o.units,0) ELSE 0 END) total_adjusted_units_plus,
            SUM(CASE WHEN o.ref_type = 'ADJUSTMENT_OUT' THEN COALESCE(o.units,0) ELSE 0 END) total_adjusted_units_minus,
            'TOTALADJUSTMENTS' METRICS_CALCULATED_FOR
            FROM
            (
                SELECT ACTUAL_RCPT_SHIP_DATE,
                source_system_code,
                customer_id,
                hdr_internal_id,
                facility_id,
                REF_TYPE,
                units,
                whse_lot_number
                FROM orion_edw.inventory_consolidated
                WHERE facility_id = pfacilityid
                AND source_system_code = psourcesystemcode
                AND REF_TYPE IN ('ADJUSTMENT IN', 'ADJUSTMENT OUT')
                AND date(ACTUAL_RCPT_SHIP_DATE) >= date(pstartdate)
                AND date(ACTUAL_RCPT_SHIP_DATE) <= date(penddate)
            ) i
            JOIN
            (
                SELECT
                tx.source_system_code,
                tx.facility_id,
                tx.hdr_internal_id,
                tx.ref_type,
                tx.transaction_date,
                tx.customer_id,
                tx.whse_lot_number,
                tx.units
                FROM orion_edw.inventory_transaction tx
                WHERE tx.facility_id = pfacilityid
                AND tx.source_system_code = psourcesystemcode
                AND trunc(tx.transaction_date) >= date(pstartdate)
                AND trunc(tx.transaction_date) <= date(penddate)
                AND tx.ref_type IN ('ADJUSTMENT IN', 'ADJUSTMENT OUT')
            ) o
            ON o.source_system_code = i.source_system_code
            AND i.facility_id = o.facility_id
            AND i.hdr_internal_id = o.hdr_internal_id
            AND i.REF_TYPE = o.REF_TYPE
            AND i.whse_lot_number = o.whse_lot_number
            AND i.customer_id = o.customer_id
            WHERE i.facility_id = pfacilityid
            AND i.source_system_code = psourcesystemcode
            AND date(o.transaction_date) >= date(pstartdate)
            AND date(o.transaction_date) <= date(penddate)
            GROUP BY REPORT_DATE, i.SOURCE_SYSTEM_CODE, i.FACILITY_ID, i.CUSTOMER_ID
        );
GET DIAGNOSTICS vROWS_AFFECTED:= ROW_COUNT;
CALL ORION_EDW.AMERICOLD_JOB_LOG(vPROC_NAME,'OK','Insert TOTALADJUSTMENTS INTO table CUST_KPI_NEW_MERGE_STAGING for GLUE_JOB_ID=' || pGLUE_JOB_UID,'CUST_KPI_NEW_MERGE_STAGING',vROWS_AFFECTED,'I');

--AMCOUTCUTS
    INSERT INTO orion_edw.cust_kpi_new_merge_staging (REPORT_DATE, SOURCE_SYSTEM_CODE, FACILITY_ID, CUSTOMER_ID, TOTAL_AMC_OUT_CUT_UNITS, METRICS_CALCULATED_FOR)
        SELECT date(REPORT_DATE) REPORT_DATE,
        SOURCE_SYSTEM_CODE,
        FACILITY_ID,
        coalesce(CUSTOMER_ID, '-') CUSTOMER_ID,
        SUM(OUT_CUT_UNITS) TOTAL_AMC_OUT_CUT_UNITS,
        'AMCOUTCUTS' AS METRICS_CALCULATED_FOR
        From
        (
            SELECT distinct date(i.ACTUAL_RCPT_SHIP_DATE) as report_date,
            i.source_system_code,
            i.facility_id,
            coalesce(i.customer_id, '-') customer_id,
            o.hdr_internal_id,
            o.internal_id,
            o.out_cut_units
            FROM
            (
                SELECT ACTUAL_RCPT_SHIP_DATE,
                source_system_code,
                customer_id,
                hdr_internal_id,
                facility_id,
                REF_TYPE
                FROM orion_edw.inventory_consolidated
                WHERE facility_id = pfacilityid
                AND source_system_code = psourcesystemcode
                AND REF_TYPE = 'ORDER'
                AND date(ACTUAL_RCPT_SHIP_DATE) >= date(pstartdate)
                AND date(ACTUAL_RCPT_SHIP_DATE) <= date(penddate)
            ) i
            JOIN
            (
                SELECT DISTINCT
                tx.source_system_code,
                tx.document_type,
                tx.facility_id,
                dtl.hdr_internal_id,
                dtl.internal_id,
                tx.day_of_ship_rcv,
                AVG(nvl(dtl.cut_units,0) ) OVER(PARTITION BY dtl.internal_id) out_cut_units
                FROM orion_edw.document_hdr tx
                JOIN orion_edw.document_dtl dtl
                    ON tx.source_system_code = dtl.source_system_code
                    AND tx.facility_id = dtl.facility_id
                    AND tx.internal_id = dtl.hdr_internal_id
                    AND tx.document_type = dtl.document_type
                JOIN orion_edw.CFG_CUSTOMER_SETTINGS C
                    ON DTL.SOURCE_SYSTEM_CODE = C.SOURCE_SYSTEM_CODE
                    AND (DTL.FACILITY_ID = C.FACILITY_ID OR C.FACILITY_ID in ('-', '%'))
                    AND (DTL.CUSTOMER_ID = C.CUSTOMER_ID OR C.CUSTOMER_ID in ('-', '----', '/'))
                    AND DTL.CUT_REASON_CODE = C.VALUE
                    AND C.KEY_NM = 'AMC_CUT_CODES'
                    AND C.MISC1 = 'TRUE'
                    AND C.MISC2 <> 2
                WHERE tx.facility_id = pfacilityid
                AND tx.source_system_code = psourcesystemcode
                AND tx.day_of_ship_rcv >= date(pstartdate)
                AND tx.day_of_ship_rcv <= date(penddate)
                AND tx.document_type = 'ORDER'
                AND dtl.cut_units > 0
            ) o
            ON o.source_system_code = i.source_system_code
            AND i.facility_id = o.facility_id
            AND i.hdr_internal_id = o.hdr_internal_id
            where i.facility_id = pfacilityid
            AND i.source_system_code = psourcesystemcode
            AND i.REF_TYPE = 'ORDER'
            AND date(i.ACTUAL_RCPT_SHIP_DATE) >= date(pstartdate)
            AND date(i.ACTUAL_RCPT_SHIP_DATE) <= date(penddate)
        )
        GROUP BY REPORT_DATE, SOURCE_SYSTEM_CODE, FACILITY_ID, CUSTOMER_ID;
GET DIAGNOSTICS vROWS_AFFECTED:= ROW_COUNT;
CALL ORION_EDW.AMERICOLD_JOB_LOG(vPROC_NAME,'OK','Insert AMCOUTCUTS INTO table CUST_KPI_NEW_MERGE_STAGING for GLUE_JOB_ID=' || pGLUE_JOB_UID,'CUST_KPI_NEW_MERGE_STAGING',vROWS_AFFECTED,'I');

--AMCOUTCUTS
    INSERT INTO orion_edw.cust_kpi_new_merge_staging (REPORT_DATE, SOURCE_SYSTEM_CODE, FACILITY_ID, CUSTOMER_ID, AMC_ADJUSTED_UNITS_PLUS,AMC_ADJUSTED_UNITS_MINUS, METRICS_CALCULATED_FOR)
    SELECT
        DATE(o.transaction_date) as REPORT_DATE,
        i.source_system_code,
        i.facility_id,
        coalesce(i.customer_id, '-')::character varying as CUSTOMER_ID,
        SUM(CASE WHEN o.ref_type = 'ADJUSTMENT IN' THEN COALESCE(o.units, 0) ELSE 0 END) as AMC_ADJUSTED_UNITS_PLUS,
        SUM(CASE WHEN o.ref_type = 'ADJUSTMENT OUT' THEN COALESCE(o.units, 0) ELSE 0 END) as AMC_ADJUSTED_UNITS_MINUS,
        'TOTALAMCADJUSTMENTS'::character varying as METRICS_CALCULATED_FOR
    FROM
    (
        SELECT
            ACTUAL_RCPT_SHIP_DATE,
            source_system_code,
            customer_id,
            hdr_internal_id,
            facility_id,
            REF_TYPE,
            units,
            whse_lot_number
        FROM orion_edw.inventory_consolidated
        WHERE
            facility_id = pfacilityid
            AND source_system_code = psourcesystemcode
            AND REF_TYPE IN ('ADJUSTMENT IN', 'ADJUSTMENT OUT')
            AND DATE(ACTUAL_RCPT_SHIP_DATE) >= pstartdate
            AND DATE(ACTUAL_RCPT_SHIP_DATE) <= penddate
    ) i
    JOIN
    (
        SELECT
            tx.source_system_code,
            tx.facility_id,
            tx.hdr_internal_id,
            tx.ref_type,
            tx.transaction_date,
            tx.customer_id,
            tx.whse_lot_number,
            tx.units
        FROM orion_edw.inventory_transaction tx
        JOIN orion_edw.CFG_CUSTOMER_SETTINGS C ON TX.SOURCE_SYSTEM_CODE = C.SOURCE_SYSTEM_CODE
        AND (TX.FACILITY_ID = C.FACILITY_ID OR C.FACILITY_ID in ('-', '%'))
        AND (TX.CUSTOMER_ID = C.CUSTOMER_ID OR C.CUSTOMER_ID in ('-', '----', '/'))
        AND TX.REASON_CODE = C.VALUE
        AND C.KEY_NM = 'AMC_ADJUSTMENT_CODES'
        AND C.MISC1 = 'TRUE'
        AND C.MISC2 <> 2
        WHERE
            tx.source_system_code = psourcesystemcode
            AND tx.facility_id = pfacilityid
            AND DATE(tx.transaction_date) >= pstartdate
            AND DATE(tx.transaction_date) <= penddate
            AND tx.ref_type IN ('ADJUSTMENT IN', 'ADJUSTMENT OUT')
    ) o
    ON o.source_system_code = i.source_system_code
    AND i.facility_id = o.facility_id
    AND i.hdr_internal_id = o.hdr_internal_id
    AND i.ref_type = o.ref_type
    AND i.customer_id = o.customer_id
    AND i.whse_lot_number = o.whse_lot_number
    GROUP BY
        DATE(o.transaction_date),
        i.source_system_code,
        i.facility_id,
        i.customer_id;
GET DIAGNOSTICS vROWS_AFFECTED:= ROW_COUNT;
CALL ORION_EDW.AMERICOLD_JOB_LOG(vPROC_NAME,'OK','Insert TOTALAMCADJUSTMENTS INTO table CUST_KPI_NEW_MERGE_STAGING for GLUE_JOB_ID=' || pGLUE_JOB_UID,'CUST_KPI_NEW_MERGE_STAGING',vROWS_AFFECTED,'I');

CALL ORION_EDW.AMERICOLD_JOB_LOG(vPROC_NAME,'OK','End Proc for GLUE_JOB_ID=' || pGLUE_JOB_UID,null,0,null) ;


EXCEPTION
WHEN OTHERS THEN
vMESSAGE:= 'Error on ' || vPROC_NAME || ' for GLUE_JOB_ID=' || pGLUE_JOB_UID || ' => Error: ' || SQLERRM;
CALL ORION_EDW.AMERICOLD_JOB_LOG(vPROC_NAME,'ERROR',vMESSAGE,NULL,0,NULL) ;
END;
$$
