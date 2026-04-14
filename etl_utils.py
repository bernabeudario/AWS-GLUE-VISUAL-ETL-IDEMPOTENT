import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrameCollection
from awsglue.dynamicframe import DynamicFrame

def insert_running_status(glueContext, collection) -> None:
    """
    Inserta un registro con status 'running' en la tabla de auditoría para el ETL dado.
    """
    spark = glueContext.spark_session

    # Extraer valores de la collection
    df = collection['output'].toDF()
    row = df.collect()[0]  # Asumiendo una sola fila
    run_uuid = row['param_run_uuid']
    etl_name = row['param_etl_name']
    backfill = row['param_backfill']
    catchup = row['param_catchup']
    start_date = row['param_start_date']
    end_date = row['param_end_date']

    try:
        spark.sql("REFRESH TABLE glue_catalog.metadata.etl_audit")
        spark.sql(f"""
            INSERT INTO glue_catalog.metadata.etl_audit 
            (uuid, etl_name, status, start_date, end_date, catchup, backfill, updated_at)
            VALUES (
                '{run_uuid}',
                '{etl_name}',
                'running',
                CAST('{start_date}' AS DATE),
                CAST('{end_date}' AS DATE),
                CAST('{catchup}' AS BOOLEAN),
                CAST('{backfill}' AS BOOLEAN),
                current_timestamp()
            )
        """)
        
    except Exception as e:
        print(f"!!! ERROR AL INSERTAR STATUS RUNNING: {str(e)}")
        raise e
    
def update_success_status(glueContext, run_uuid) -> None:
    """
    Actualiza el status del registro con el valor 'success' en la tabla de auditoría para el uuid dado.
    """
    spark = glueContext.spark_session

    try:
        spark.sql("REFRESH TABLE glue_catalog.metadata.etl_audit")
        spark.sql(f"""
            UPDATE glue_catalog.metadata.etl_audit
            SET status = 'success', updated_at = current_timestamp()
            WHERE uuid = '{run_uuid}'
        """)
    except Exception as e:
        print(f"!!! ERROR AL ACTUALIZAR STATUS SUCCESS: {str(e)}")
        raise e
    
def update_failed_status(glueContext, run_uuid) -> None:
    """
    Actualiza el status del registro con el valor 'failed' en la tabla de auditoría para el uuid dado.
    """
    spark = glueContext.spark_session

    try:
        spark.sql("REFRESH TABLE glue_catalog.metadata.etl_audit")
        spark.sql(f"""
            UPDATE glue_catalog.metadata.etl_audit
            SET status = 'failed', updated_at = current_timestamp()
            WHERE uuid = '{run_uuid}'
        """)
    except Exception as e:
        print(f"!!! ERROR AL ACTUALIZAR STATUS FAILED: {str(e)}")
        raise e

def calculate_execution_horizons(glueContext, params) -> DynamicFrameCollection:
    """
    Valida auditorías previas y calcula los horizontes de fechas para la ejecución del ETL.
    Retorna una tupla con: (execute, p_start_date, p_end_date, p_catchup, p_backfill, p_etl_name)
    """
    from pyspark.sql import functions as F
    from pyspark.sql.types import StructType, StructField, StringType, DateType
    from datetime import datetime, timedelta
    
    spark = glueContext.spark_session

    p_run_uuid = params['run_uuid']
    p_etl_name = params['etl_name']
    p_backfill = params['backfill']
    p_catchup = params['catchup'] 

    if p_backfill == 'true' and p_catchup == 'true':
        raise ValueError("Los parámetros 'catchup' y 'backfill' son mutuamente excluyentes.")

    # Verifica si el ETL ya se está ejecutando
    is_running_df = spark.sql(f"""
        SELECT COUNT(*) as cnt
        FROM glue_catalog.metadata.etl_audit 
        WHERE 1 = 1
        AND etl_name = '{p_etl_name}' 
        AND status = 'running'
    """)
    is_running = is_running_df.collect()[0]['cnt']
    
    if is_running > 0:
        raise RuntimeError(f"El ETL '{p_etl_name}' ya se está ejecutando actualmente. Proceso abortado.")
        
    # Busca el último registro con backfill=false y selecciona MAX(end_date)
    max_date_df = spark.sql(f"""
        SELECT MAX(end_date) as max_date
        FROM glue_catalog.metadata.etl_audit 
        WHERE 1 = 1
        AND etl_name = '{p_etl_name}' 
        AND backfill = 'false'
        AND status = 'success'
    """)
    last_execution_date = max_date_df.collect()[0]['max_date']
    
    yesterday = (datetime.now() - timedelta(days=1)).date()    
        
    # Establecer p_start_date
    if p_backfill == 'false' and p_catchup == 'false':
        p_start_date = yesterday
        p_end_date = yesterday

    if p_catchup == 'true':
        user_start_date = datetime.strptime(params['start_date'], '%Y-%m-%d').date()
        if last_execution_date is None:
            p_start_date = user_start_date
        else:
            # Selecciona el mayor entre la fecha enviada y el último día cargado + 1
            p_start_date = max(user_start_date, last_execution_date + timedelta(days=1))
        p_end_date = yesterday

    if p_backfill == 'true':
        p_start_date = datetime.strptime(params['start_date'], '%Y-%m-%d').date()
        p_end_date = datetime.strptime(params['end_date'], '%Y-%m-%d').date()

    # Lógica 'execute'
    execute = 'true'
    if p_start_date > p_end_date:
        execute = 'false'
        
    # Generación de resultado
    result_data = []
    if execute == 'true':
        result_data.append((p_etl_name, p_backfill, p_catchup, p_start_date, p_end_date, p_run_uuid))

    schema_out = StructType([
        StructField("param_etl_name", StringType(), True),
        StructField("param_backfill", StringType(), True),
        StructField("param_catchup", StringType(), True),
        StructField("param_start_date", DateType(), True),
        StructField("param_end_date", DateType(), True),
        StructField("param_run_uuid", StringType(), True)
    ])
    
    df_result = spark.createDataFrame(result_data, schema=schema_out)
    dyf_result = DynamicFrame.fromDF(df_result, glueContext, "dyf_result")

    # Retornamos colección con inyección de métodos para compatibilidad visual
    collection = DynamicFrameCollection({"output": dyf_result}, glueContext)
    for method in ['toDF', 'getNumPartitions', 'schema', 'printSchema', 'show']:
        setattr(collection, method, getattr(dyf_result, method))

    return collection