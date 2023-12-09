CREATE OR REPLACE PROCEDURE orion_edw.proc_custkpi_man(p_report_date date, pglue_job_uid character varying(100))
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
BEGIN
 vEXEC_ID:=PG_BACKEND_PID();
 vPROC_NAME:= 'PROC_CUSTKPI_MAN';

 CALL ORION_EDW.AMERICOLD_JOB_LOG(vPROC_NAME,'OK','Start Proc for GLUE_JOB_ID=' || pGLUE_JOB_UID,null,0,null) ;

DELETE "lvi3plp-stg"."orion_edw"."cust_kpi_man"
WHERE REPORT_DATE=p_report_date;

GET DIAGNOSTICS vROWS_AFFECTED:= ROW_COUNT;
CALL ORION_EDW.AMERICOLD_JOB_LOG(vPROC_NAME,'OK','Delete CUST_KPI_MAN for Period ' || to_char(p_report_date,'YYYYMMDD') || '  for GLUE_JOB_ID=' || pGLUE_JOB_UID,'CUST_KPI_MAN',vROWS_AFFECTED,'D') ;



    INSERT INTO "lvi3plp-stg"."orion_edw"."cust_kpi_man" (
        report_date,
		source_system_code,
		facility_id,
		customer_id,
		current_pallet_capacity,
		current_units,
		current_net_weight,
		current_gross_weight,
		current_volume,
		current_pallets,
		current_pallet_occupancy,
		in_units,
		in_net_weight,
		in_gross_weight,
		in_catch_weight,
		in_volume,
		in_total_receipts,
		in_pallets,
		out_units,
		out_unit_pick_units,
		out_net_weight,
		out_gross_weight,
		out_catch_weight,
		out_volume,
		out_total_orders,
		out_pallets,
		out_cut_units,
		out_casepick_unit_qty,
		out_casepick_catch_qty,
		out_casepick_gross_weight,
		out_casepick_pallets,
		out_palpick_unit_qty,
		out_palpick_catch_qty,
		out_palpick_gross_weight,
		out_palpick_pallets,
		out_total_lpns,
		item_count,
		whse_lot_count,
		total_amc_out_cut_units,
		total_amc_adjusted_units,
		creator_id,
		last_updater_id,
		create_date,
		last_update_date,
		avg_lead_time,
		in_loads_total,
		in_no_show_appointments,
		in_late_appointments,
		in_total_turn_time,
		in_total_dwell_time,
		in_loads_live,
		in_loads_live_late,
		in_loads_live_not_turned_ot,
		in_loads_drop,
		in_loads_drop_late,
		in_loads_drop_not_turned_ot,
		in_loads_rail,
		in_loads_rail_late,
		in_loads_rail_not_turned_ot,
		in_loads_floor_loaded,
		in_floor_loads_ntot,
		out_loads_total,
		out_no_show_appointments,
		out_late_appointments,
		out_total_turn_time,
		out_total_dwell_time,
		out_loads_live,
		out_loads_live_late,
		out_loads_live_not_turned_ot,
		out_loads_drop,
		out_loads_drop_late,
		out_loads_drop_not_turned_ot,
		out_loads_rail,
		out_loads_rail_late,
		out_loads_rail_not_turned_ot,
		out_loads_floor_loaded,
		out_floor_loads_ntot,
		sap_customer_id,
		out_receipt_case_pick_calc,
		out_billing_case_pick_calc,
		in_loads_live_late_ntot,
		in_loads_drop_late_ntot,
		in_loads_rail_late_ntot,
		in_floor_loads_late_ntot,
		out_loads_live_late_ntot,
		out_loads_drop_late_ntot,
		out_loads_rail_late_ntot,
		out_floor_loads_late_ntot,
		in_loads_live_deprioritized,
		in_loads_drop_deprioritized,
		in_loads_rail_deprioritized,
		in_floor_loads_deprioritized,
		out_loads_live_deprioritized,
		out_loads_drop_deprioritized,
		out_loads_rail_deprioritized,
		out_floor_loads_deprioritized,
		amc_adjusted_units_plus,
		amc_adjusted_units_minus,
		total_adjusted_units_plus,
		total_adjusted_units_minus,
		total_adjusted_units,
		timezone_uom,
		weight_uom,
		dimension_uom,
		volume_uom,
		distance_uom,
		temperature_uom,
		monetary_uom,
		au_created_date
    )
    SELECT
        CAST (kpi_list_kpi_report_date as date) as report_date
		,kpi_list_kpi_source_system_code as source_system_code
		,kpi_list_kpi_facility_id as facility_id
		,kpi_list_kpi_customer_details_customer_id as customer_id
		,kpi_list_kpi_pallet_capacity as current_pallet_capacity
		,kpi_list_kpi_customer_details_current_units as current_units
		,kpi_list_kpi_customer_details_current_net_weight as current_net_weight
		,kpi_list_kpi_customer_details_current_gross_weight as current_gross_weight
		,kpi_list_kpi_customer_details_current_volume as current_volume
		,kpi_list_kpi_customer_details_current_pallets as current_pallets
		,kpi_list_kpi_customer_details_current_pallet_occupancy_total as current_pallet_occupancy
		,kpi_list_kpi_customer_details_inbound_units as in_units
		,kpi_list_kpi_customer_details_inbound_net_weight as in_net_weight
		,kpi_list_kpi_customer_details_inbound_gross_weight as in_gross_weight
		,kpi_list_kpi_customer_details_inbound_catch_weight as in_catch_weight
		,kpi_list_kpi_customer_details_inbound_volume as in_volume
		,kpi_list_kpi_customer_details_inbound_total_documents as in_total_receipts
		,kpi_list_kpi_customer_details_inbound_pallets as in_pallets
		,kpi_list_kpi_customer_details_outbound_units as out_units
		,kpi_list_kpi_customer_details_outbound_case_pick_units as out_case_pick_units
		,kpi_list_kpi_customer_details_outbound_net_weight as out_net_weight
		,kpi_list_kpi_customer_details_outbound_gross_weight as out_gross_weight
		,kpi_list_kpi_customer_details_outbound_catch_weight as out_catch_weight
		,kpi_list_kpi_customer_details_outbound_volume as out_volume
		,kpi_list_kpi_customer_details_outbound_total_documents as out_total_orders
		,kpi_list_kpi_customer_details_outbound_pallets as out_pallets
		,kpi_list_kpi_customer_details_outbound_cut_units as out_cut_units
		,kpi_list_kpi_customer_details_outbound_partial_pick_units as out_casepick_unit_qty
		,kpi_list_kpi_customer_details_outbound_partial_pick_catch_weight as out_casepick_catch_qty
		,kpi_list_kpi_customer_details_outbound_partial_pick_gross_weight as out_casepick_gross_weight
		,kpi_list_kpi_customer_details_outbound_partial_pick_pallets as out_casepick_pallets
		,kpi_list_kpi_customer_details_outbound_full_pallet_pick_units as out_palpick_unit_qty
		,kpi_list_kpi_customer_details_outbound_full_pallet_pick_catch_weight as out_palpick_catch_qty
		,kpi_list_kpi_customer_details_outbound_full_pallet_pick_gross_weight as out_palpick_gross_weight
		,kpi_list_kpi_customer_details_outbound_full_pallet_pick_pallets as out_palpick_pallets
		,kpi_list_kpi_customer_details_outbound_total_lpn as out_total_lpns
		,kpi_list_kpi_customer_details_item_count as item_count
		,kpi_list_kpi_customer_details_whse_lot_count as whse_lot_count
		,kpi_list_kpi_customer_details_outbound_amc_cut_units as total_amc_out_cut_units
		,kpi_list_kpi_customer_details_adjustment_amc_adjusted_units_total as total_amc_adjusted_units
		,kpi_list_kpi_customer_details_creator_id as creator_id
		,kpi_list_kpi_customer_details_last_updater_id as last_updater_id
		,CAST(kpi_list_kpi_customer_details_create_date as timestamp) as create_date
		,CAST(kpi_list_kpi_customer_details_last_update_date as timestamp) as last_update_date
		,COALESCE(CAST(NULLIF('APPT_PLACEHOLDER', 'APPT_PLACEHOLDER') AS numeric), 0) as avg_lead_time
		,COALESCE(CAST(NULLIF('APPT_PLACEHOLDER', 'APPT_PLACEHOLDER') AS numeric), 0) as in_loads_total
		,COALESCE(CAST(NULLIF('APPT_PLACEHOLDER', 'APPT_PLACEHOLDER') AS numeric), 0) as in_no_show_appointments
		,COALESCE(CAST(NULLIF('APPT_PLACEHOLDER', 'APPT_PLACEHOLDER') AS numeric), 0) as in_late_appointments
		,COALESCE(CAST(NULLIF('APPT_PLACEHOLDER', 'APPT_PLACEHOLDER') AS numeric), 0) as in_total_turn_time
		,COALESCE(CAST(NULLIF('APPT_PLACEHOLDER', 'APPT_PLACEHOLDER') AS numeric), 0) as in_total_dwell_time
		,COALESCE(CAST(NULLIF('APPT_PLACEHOLDER', 'APPT_PLACEHOLDER') AS numeric), 0) as in_loads_live
		,COALESCE(CAST(NULLIF('APPT_PLACEHOLDER', 'APPT_PLACEHOLDER') AS numeric), 0) as in_loads_live_late
		,COALESCE(CAST(NULLIF('APPT_PLACEHOLDER', 'APPT_PLACEHOLDER') AS numeric), 0) as in_loads_live_not_turned_ot
		,COALESCE(CAST(NULLIF('APPT_PLACEHOLDER', 'APPT_PLACEHOLDER') AS numeric), 0) as in_loads_drop
		,COALESCE(CAST(NULLIF('APPT_PLACEHOLDER', 'APPT_PLACEHOLDER') AS numeric), 0) as in_loads_drop_late
		,COALESCE(CAST(NULLIF('APPT_PLACEHOLDER', 'APPT_PLACEHOLDER') AS numeric), 0) as in_loads_drop_not_turned_ot
		,COALESCE(CAST(NULLIF('APPT_PLACEHOLDER', 'APPT_PLACEHOLDER') AS numeric), 0) as in_loads_rail
		,COALESCE(CAST(NULLIF('APPT_PLACEHOLDER', 'APPT_PLACEHOLDER') AS numeric), 0) as in_loads_rail_late
		,COALESCE(CAST(NULLIF('APPT_PLACEHOLDER', 'APPT_PLACEHOLDER') AS numeric), 0) as in_loads_rail_not_turned_ot
		,COALESCE(CAST(NULLIF('APPT_PLACEHOLDER', 'APPT_PLACEHOLDER') AS numeric), 0) as in_loads_floor_loaded
		,COALESCE(CAST(NULLIF('APPT_PLACEHOLDER', 'APPT_PLACEHOLDER') AS numeric), 0) as in_floor_loads_ntot
		,COALESCE(CAST(NULLIF('APPT_PLACEHOLDER', 'APPT_PLACEHOLDER') AS numeric), 0) as out_loads_total
		,COALESCE(CAST(NULLIF('APPT_PLACEHOLDER', 'APPT_PLACEHOLDER') AS numeric), 0) as out_no_show_appointments
		,COALESCE(CAST(NULLIF('APPT_PLACEHOLDER', 'APPT_PLACEHOLDER') AS numeric), 0) as out_late_appointments
		,COALESCE(CAST(NULLIF('APPT_PLACEHOLDER', 'APPT_PLACEHOLDER') AS numeric), 0) as out_total_turn_time
		,COALESCE(CAST(NULLIF('APPT_PLACEHOLDER', 'APPT_PLACEHOLDER') AS numeric), 0) as out_total_dwell_time
		,COALESCE(CAST(NULLIF('APPT_PLACEHOLDER', 'APPT_PLACEHOLDER') AS numeric), 0) as out_loads_live
		,COALESCE(CAST(NULLIF('APPT_PLACEHOLDER', 'APPT_PLACEHOLDER') AS numeric), 0) as out_loads_live_late
		,COALESCE(CAST(NULLIF('APPT_PLACEHOLDER', 'APPT_PLACEHOLDER') AS numeric), 0) as out_loads_live_not_turned_ot
		,COALESCE(CAST(NULLIF('APPT_PLACEHOLDER', 'APPT_PLACEHOLDER') AS numeric), 0) as out_loads_drop
		,COALESCE(CAST(NULLIF('APPT_PLACEHOLDER', 'APPT_PLACEHOLDER') AS numeric), 0) as out_loads_drop_late
		,COALESCE(CAST(NULLIF('APPT_PLACEHOLDER', 'APPT_PLACEHOLDER') AS numeric), 0) as out_loads_drop_not_turned_ot
		,COALESCE(CAST(NULLIF('APPT_PLACEHOLDER', 'APPT_PLACEHOLDER') AS numeric), 0) as out_loads_rail
		,COALESCE(CAST(NULLIF('APPT_PLACEHOLDER', 'APPT_PLACEHOLDER') AS numeric), 0) as out_loads_rail_late
		,COALESCE(CAST(NULLIF('APPT_PLACEHOLDER', 'APPT_PLACEHOLDER') AS numeric), 0) as out_loads_rail_not_turned_ot
		,COALESCE(CAST(NULLIF('APPT_PLACEHOLDER', 'APPT_PLACEHOLDER') AS numeric), 0) as out_loads_floor_loaded
		,COALESCE(CAST(NULLIF('APPT_PLACEHOLDER', 'APPT_PLACEHOLDER') AS numeric), 0) as out_floor_loads_ntot
		,kpi_list_kpi_customer_details_sap_customer_id as sap_customer_id
		,COALESCE(CAST(NULLIF('APPT_PLACEHOLDER', 'APPT_PLACEHOLDER') AS numeric), 0) as out_receipt_case_pick_calc
		,COALESCE(CAST(NULLIF('APPT_PLACEHOLDER', 'APPT_PLACEHOLDER') AS numeric), 0) as out_billing_case_pick_calc
		,COALESCE(CAST(NULLIF('APPT_PLACEHOLDER', 'APPT_PLACEHOLDER') AS numeric), 0) as in_loads_live_late_ntot
		,COALESCE(CAST(NULLIF('APPT_PLACEHOLDER', 'APPT_PLACEHOLDER') AS numeric), 0) as in_loads_drop_late_ntot
		,COALESCE(CAST(NULLIF('APPT_PLACEHOLDER', 'APPT_PLACEHOLDER') AS numeric), 0) as in_loads_rail_late_ntot
		,COALESCE(CAST(NULLIF('APPT_PLACEHOLDER', 'APPT_PLACEHOLDER') AS numeric), 0) as in_floor_loads_late_ntot
		,COALESCE(CAST(NULLIF('APPT_PLACEHOLDER', 'APPT_PLACEHOLDER') AS numeric), 0) as out_loads_live_late_ntot
		,COALESCE(CAST(NULLIF('APPT_PLACEHOLDER', 'APPT_PLACEHOLDER') AS numeric), 0) as out_loads_drop_late_ntot
		,COALESCE(CAST(NULLIF('APPT_PLACEHOLDER', 'APPT_PLACEHOLDER') AS numeric), 0) as out_loads_rail_late_ntot
		,COALESCE(CAST(NULLIF('APPT_PLACEHOLDER', 'APPT_PLACEHOLDER') AS numeric), 0) as out_floor_loads_late_ntot
		,COALESCE(CAST(NULLIF('APPT_PLACEHOLDER', 'APPT_PLACEHOLDER') AS numeric), 0) as in_loads_live_deprioritized
		,COALESCE(CAST(NULLIF('APPT_PLACEHOLDER', 'APPT_PLACEHOLDER') AS numeric), 0) as in_loads_drop_deprioritized
		,COALESCE(CAST(NULLIF('APPT_PLACEHOLDER', 'APPT_PLACEHOLDER') AS numeric), 0) as in_loads_rail_deprioritized
		,COALESCE(CAST(NULLIF('APPT_PLACEHOLDER', 'APPT_PLACEHOLDER') AS numeric), 0) as in_floor_loads_deprioritized
		,COALESCE(CAST(NULLIF('APPT_PLACEHOLDER', 'APPT_PLACEHOLDER') AS numeric), 0) as out_loads_live_deprioritized
		,COALESCE(CAST(NULLIF('APPT_PLACEHOLDER', 'APPT_PLACEHOLDER') AS numeric), 0) as out_loads_drop_deprioritized
		,COALESCE(CAST(NULLIF('APPT_PLACEHOLDER', 'APPT_PLACEHOLDER') AS numeric), 0) as out_loads_rail_deprioritized
		,COALESCE(CAST(NULLIF('APPT_PLACEHOLDER', 'APPT_PLACEHOLDER') AS numeric), 0) as out_floor_loads_deprioritized
		,kpi_list_kpi_customer_details_adjustment_amc_adjusted_units_plus as amc_adjusted_units_plus
		,kpi_list_kpi_customer_details_adjustment_amc_adjusted_units_minus as amc_adjusted_units_minus
		,kpi_list_kpi_customer_details_adjustment_total_adjusted_units_plus as total_adjusted_units_plus
		,kpi_list_kpi_customer_details_adjustment_total_adjusted_units_minus as total_adjusted_units_minus
		,kpi_list_kpi_customer_details_adjustment_total_adjusted_units_total as total_adjusted_units
		,kpi_list_kpi_customer_details_uoms_date_timezone as timezone_uom
		,kpi_list_kpi_customer_details_uoms_weight_uom as weight_uom
		,kpi_list_kpi_customer_details_uoms_dimension_uom as dimension_uom
		,kpi_list_kpi_customer_details_uoms_volume_uom as volume_uom
		,kpi_list_kpi_customer_details_uoms_distance_uom as distance_uom
		,kpi_list_kpi_customer_details_uoms_temperature_uom as temperature_uom
		,kpi_list_kpi_customer_details_uoms_monetary_uom as monetary_uom
		,getdate()
    FROM
        (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY SPLIT_PART(filename, '_', 1), SPLIT_PART(filename, '_', 2) ORDER BY SPLIT_PART(filename, '_', 3) DESC) AS keep
        FROM
            ORION_EDW.parsed_manual_kpi_stg_ver3
        WHERE
            CAST(kpi_list_kpi_report_date AS DATE) = p_report_date
        ) a
    WHERE
        a.keep = 1;

GET DIAGNOSTICS vROWS_AFFECTED:= ROW_COUNT;
CALL ORION_EDW.AMERICOLD_JOB_LOG(vPROC_NAME,'OK','Insert on CUST_KPI_MAN table for period ' || to_char(p_report_date,'YYYYMMDD') || ' for GLUE_JOB_ID=' || pGLUE_JOB_UID,'CUST_KPI_MAN',vROWS_AFFECTED,'I') ;

CALL ORION_EDW.AMERICOLD_JOB_LOG(vPROC_NAME,'OK','End Proc for GLUE_JOB_ID=' || pGLUE_JOB_UID,null,0,null) ;

EXCEPTION
WHEN OTHERS THEN
	vMESSAGE:= 'Error on ' || vPROC_NAME || ' for GLUE_JOB_ID=' || pGLUE_JOB_UID || ' => Error: ' || SQLERRM;
	CALL ORION_EDW.AMERICOLD_JOB_LOG(vPROC_NAME,'ERROR',vMESSAGE,NULL,0,NULL) ;
END
$$