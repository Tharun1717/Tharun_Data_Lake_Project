CREATE OR REPLACE PROCEDURE orion_edw.americold_job_log(pproc_name character varying(100), presult character varying(100), pmessage character varying(1000), pdb_object_affected character varying(256), prows_affected numeric(18,0), poperation character varying(100))
 LANGUAGE plpgsql
AS $$

/****************************************************************************************
    Author: Martin Esteves
    Creation Date: 2023-02-23
    Description:
        Procedure that will be logging steps in SVL_STORED_PROC_MESSAGES table using RAISE function.
    Execution Sample:
        DECLARE
            vPROC_NAME VARCHAR(100);
            vMESSAGE VARCHAR(1000);
            vROWS_AFFECTED NUMERIC(18,0);
            vDB_OBJ_NAME VARCHAR(100);
        BEGIN
            vPROC_NAME:='proc_name';
            vROWS_AFFECTED:=0;
            vDB_OBJ_NAME:='';

            --First Temp log entry to generate and preserve the Unique identifier of the execution
            CALL ORION_EDW.AMERICOLD_JOB_LOG(vPROC_NAME,'OK',vMESSAGE,vDB_OBJ_NAME,vROWS_AFFECTED,null) ;            --Log Entry
            vMESSAGE:='Test Message';
            CALL ORION_EDW.AMERICOLD_JOB_LOG(vPROC_NAME,'OK',vMESSAGE,vDB_OBJ_NAME,vROWS_AFFECTED,null) ;        END;

*****************************************************************************************/

DECLARE
vDB_OBJECT_AFFECTED varchar(100);

BEGIN
    vDB_OBJECT_AFFECTED:=lower(PDB_OBJECT_AFFECTED);
    raise info 'AMERICOLD LOG | PROC_NAME:% | RESULT:% | MESSAGE:% | DB_OBJECT_AFFECTED:% | ROWS_AFFECTED:% | OPERATION:%', pPROC_NAME, pRESULT, pMESSAGE, vDB_OBJECT_AFFECTED, pROWS_AFFECTED, pOPERATION;

EXCEPTION WHEN OTHERS THEN
    RAISE INFO 'AMERICOLD LOG | PROC_NAME:% | RESULT:ERROR | MESSAGE:% | DB_OBJECT_AFFECTED:% | ROWS_AFFECTED:0 | OPERATION:E', pPROC_NAME, pRESULT, SQLERRM, vDB_OBJECT_AFFECTED, pROWS_AFFECTED, pOPERATION;
END;
$$
