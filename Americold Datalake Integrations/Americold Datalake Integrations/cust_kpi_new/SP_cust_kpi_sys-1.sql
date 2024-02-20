CREATE OR REPLACE PROCEDURE orion_edw.proc_custkpi_processor_occupancy(psourcesystemcode character varying(256), pfacility_id character varying(256), pstartdate character varying(256), penddate character varying(256), pglue_job_uid character varying(100))
 LANGUAGE plpgsql
AS $$

DECLARE
  v_section CHARACTER VARYING(200) := 'INIT';
  vEXEC_ID NUMERIC(18, 0);
  vPROC_NAME VARCHAR(1000);
  vMESSAGE VARCHAR(1000);
  vROWS_AFFECTED NUMERIC(18, 0);
  vREC NUMERIC(18, 0);
  vRUN_DATE_TZ TIMESTAMPTZ;
BEGIN
  vEXEC_ID:=PG_BACKEND_PID();
  vPROC_NAME:= 'PROC_CUSTKPI_PROCESSOR_OCUPPANCY';

  CALL ORION_EDW.AMERICOLD_JOB_LOG(vPROC_NAME,'OK','Start Proc for GLUE_JOB_ID=' || pGLUE_JOB_UID,null,0,null) ;

  INSERT INTO orion_edw.pallet_occupancy_kpi(facility_id,source_system_code,storage_type,customer_id,report_date,customer_occupancy_total,receipt_occupancy_total,lpn_occupancy_total,last_processed)
      SELECT
            ic.facility_id,
            ic.source_system_code,
            UPPER(COALESCE(CASE storage_type
                WHEN 'C' THEN 'COOLER'
                WHEN 'F' THEN 'FREEZER'
                WHEN 'D' THEN 'DRY'
                ELSE storage_type
            END, 'N/A')) AS storage_type,
            customer_id,
            CAST(TO_CHAR(ACTUAL_RCPT_SHIP_DATE,'YYYYMMDD') as Numeric) AS report_date,
            SUM(COALESCE(billing_occupancy_count_calc, 0)) AS customer_occupancy_total,
            CASE SOURCE_SYSTEM_CODE WHEN 'PWR' THEN
                sum(CASE ref_type
                      WHEN 'STORAGE' THEN COALESCE(LPN_COUNT_CALC, 0)
                      ELSE 0
                END   )
                WHEN 'P2'
              THEN
                sum(CASE ref_type
                      WHEN 'STORAGE' THEN COALESCE(LPN_COUNT_CALC, 0)
                      ELSE 0
                  END)
                WHEN 'PAU'
              THEN
                sum(CASE ref_type
                      WHEN 'STORAGE' THEN COALESCE(LPN_COUNT_CALC, 0)
                      ELSE 0
                  END)
              ELSE
              sum(CASE ref_type
                      WHEN 'STORAGE' THEN COALESCE(receipt_occupancy_count_calc, 0) + COALESCE(mixed_occupancy_count_calc, 0)
                      ELSE 0

                    END)
                            END  receipt_occupancy_total,
            SUM(COALESCE(lpn_count_calc, 0)) AS lpn_occupancy_total,
            localtimestamp last_processed
        FROM orion_edw.inventory_consolidated AS ic
        WHERE ic.source_system_code = psourcesystemcode
              AND ic.facility_id = pfacility_id
              AND ic.ref_type = 'STORAGE'
              AND date(ic.ACTUAL_RCPT_SHIP_DATE) >= date(pstartdate)
              AND date(ic.ACTUAL_RCPT_SHIP_DATE) <= date(penddate)
            -- AND (ic.kpi_exclude_flag IS NULL)
        GROUP BY  ic.facility_id,
                  ic.source_system_code,
                  UPPER(COALESCE(CASE storage_type
                                      WHEN 'C' THEN 'COOLER'
                                      WHEN 'F' THEN 'FREEZER'
                                      WHEN 'D' THEN 'DRY'
                                      ELSE storage_type
                                  END, 'N/A')),
                  customer_id,
                  CAST (TO_CHAR(ACTUAL_RCPT_SHIP_DATE,'YYYYMMDD') as Numeric);

GET DIAGNOSTICS vROWS_AFFECTED:= ROW_COUNT;
CALL ORION_EDW.AMERICOLD_JOB_LOG(vPROC_NAME,'OK','Insert into table PALLET_OCCUPANCY_KPI for GLUE_JOB_ID=' || pGLUE_JOB_UID,'PALLET_OCCUPANCY_KPI',vROWS_AFFECTED,'I');

CALL ORION_EDW.AMERICOLD_JOB_LOG(vPROC_NAME,'OK','End Proc for GLUE_JOB_ID=' || pGLUE_JOB_UID,null,0,null) ;


EXCEPTION
WHEN OTHERS THEN
vMESSAGE:= 'Error on ' || vPROC_NAME || ' for GLUE_JOB_ID=' || pGLUE_JOB_UID || ' => Error: ' || SQLERRM;
CALL ORION_EDW.AMERICOLD_JOB_LOG(vPROC_NAME,'ERROR',vMESSAGE,NULL,0,NULL) ;


END;
$$
