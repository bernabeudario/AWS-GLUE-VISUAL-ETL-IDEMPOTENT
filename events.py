import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrameCollection
from awsglue.dynamicframe import DynamicFrame
from awsglue import DynamicFrame
from pyspark.sql import functions as SqlFuncs

# Script generated for node output
def outputTransform(glueContext, dfc) -> DynamicFrameCollection:
    from etl_utils import update_success_status, update_failed_status
    import sys
    spark = glueContext.spark_session

    # Si detecta PREVIEW MODE sale para evitar que se ejecute el código a continuación
    if not any("--JOB_NAME" in arg for arg in sys.argv):
        return dfc

    keys = list(dfc.keys())
    # Obtenemos el 1er DF que contiene los datos seleccionados y registramos vista temporal
    source_df = dfc[keys[0]].toDF()
    source_df.createOrReplaceTempView("updates_source")

    # Obtenemos el 2do DF que contiene los parámetros utilizados y extraemos los valores necesarios
    param_df = dfc[keys[1]].toDF()
    run_uuid = param_df.limit(1).collect()[0]["param_run_uuid"]
    p_start_date = param_df.limit(1).collect()[0]["param_start_date"]
    p_end_date = param_df.limit(1).collect()[0]["param_end_date"]

    # Capturamos snapshot ID actual para rollback en caso de error
    previous_snapshot_id = None

    try:
        if source_df.count() > 0:        
            # Obtener el snapshot ID actual antes de iniciar cambios
            spark.sql("REFRESH TABLE glue_catalog.silver.events")
            snapshot_df = spark.sql("""
                SELECT snapshot_id 
                FROM glue_catalog.silver.events.snapshots 
                ORDER BY committed_at DESC 
                LIMIT 1
            """)
            previous_snapshot_id = snapshot_df.collect()[0]["snapshot_id"]

            # Ejecución de queries principales
            spark.sql(f"""
                DELETE FROM glue_catalog.silver.events
                WHERE 1 = 1 
                AND event_date >= CAST('{p_start_date}' AS DATE)
                AND event_date <= CAST('{p_end_date}' AS DATE)
            """)
            spark.sql("""
                INSERT INTO glue_catalog.silver.events (uuid, event_date, user_mail, event, system)
                SELECT
                 uuid
                ,event_date
                ,user_mail
                ,event
                ,system
                FROM 
                updates_source
            """)

        # Actualización de metadatos de auditoría: success
        update_success_status(glueContext, run_uuid)

    except Exception as e:
        # Rollback a snapshot anterior
        spark.sql(f"""
            CALL glue_catalog.system.rollback_to_snapshot
                ('glue_catalog.silver.events', {previous_snapshot_id})
        """)
        
        # Actualización de metadatos de auditoría: failed
        update_failed_status(glueContext, run_uuid)
        print(f"Ejecución abortada. Excepción: {e}")
        raise

    return dfc
# Script generated for node get_parameters
def get_parametersTransform(glueContext, dfc) -> DynamicFrameCollection:
    from etl_utils import calculate_execution_horizons, insert_running_status
    from awsglue.utils import getResolvedOptions
    import sys
    import uuid

    # automático
    args = getResolvedOptions(sys.argv, ['etl_name', 'catchup', 'backfill', 'start_date', 'end_date'])

    # manual para test
    #args = {
    #    "etl_name": "events",
    #    "catchup": "false",
    #    "backfill": "true",
    #    "start_date": "2026-03-21",
    #    "end_date": "2026-03-21"
    #}
    args['run_uuid'] = str(uuid.uuid4())

    collection = calculate_execution_horizons(glueContext, args)

    # Inserta registro con status 'running' si no está en PREVIEW MODE
    if any("--JOB_NAME" in arg for arg in sys.argv):
        insert_running_status(glueContext, collection)

    return collection
def sparkSqlQuery(glueContext, query, mapping, transformation_ctx) -> DynamicFrame:
    for alias, frame in mapping.items():
        frame.toDF().createOrReplaceTempView(alias)
    result = spark.sql(query)
    return DynamicFrame.fromDF(result, glueContext, transformation_ctx)
args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Script generated for node bronze | events
bronzeevents_node1771153705382_df = glueContext.create_data_frame.from_catalog(database="bronze", table_name="events")
bronzeevents_node1771153705382 = DynamicFrame.fromDF(bronzeevents_node1771153705382_df, glueContext, "bronzeevents_node1771153705382")

# Script generated for node get_parameters
get_parameters_node1771153092307 = get_parametersTransform(glueContext, DynamicFrameCollection({"bronzeevents_node1771153705382": bronzeevents_node1771153705382}, glueContext))

# Script generated for node seleccion
SqlQuery0 = '''
SELECT 
 uuid() AS uuid
,e.event_date
,split(e.user_mail, '@')[0] AS user_mail
,UPPER(e.event) AS event
,UPPER(LEFT(e.system, 1)) AS system
FROM 
events e
CROSS JOIN parameters p
WHERE 1 = 1
AND e.event_date >= p.param_start_date
AND e.event_date <= p.param_end_date
ORDER BY e.event_date DESC
'''
seleccion_node1771153724380 = sparkSqlQuery(glueContext, query = SqlQuery0, mapping = {"events":bronzeevents_node1771153705382, "parameters":get_parameters_node1771153092307}, transformation_ctx = "seleccion_node1771153724380")

# Script generated for node drop duplicates
dropduplicates_node1771515690584 =  DynamicFrame.fromDF(seleccion_node1771153724380.toDF().dropDuplicates(["event_date", "user_mail", "event", "system"]), glueContext, "dropduplicates_node1771515690584")

# Script generated for node output
output_node1771170516536 = outputTransform(glueContext, DynamicFrameCollection({"dropduplicates_node1771515690584": dropduplicates_node1771515690584, "get_parameters_node1771153092307": get_parameters_node1771153092307}, glueContext))

job.commit()