# Databricks notebook source
# import necessaries libs
from pyspark.sql.functions import col, lit, current_date, to_date, sha1, udf, when, month, year, concat
from pyspark.sql.types     import StringType, DoubleType
from uuid                  import uuid4
import requests
import subprocess
import json

# COMMAND ----------

## parameters
# aws s3 parameters
#access_key = dbutils.secrets.get(scope = "aws", key = "aws-access-key")
#secret_key = dbutils.secrets.get(scope = "aws", key = "aws-secret-key")
#encoded_secret_key = secret_key.replace("/", "%2F")
aws_bucket_name = "stonedatalake"
mount_name = "stonedatalake"

# datalake paths
landing_path = lambda table: f"dbfs:/mnt/{mount_name}/landing/{table}"
bronze_path = lambda table: f"dbfs:/mnt/{mount_name}/bronze/{table}"
silver_path = lambda table: f"dbfs:/mnt/{mount_name}/silver/{table}"
gold_path = lambda table: f"dbfs:/mnt/{mount_name}/gold/{table}"

# COMMAND ----------

# mount s3 bucket
#dbutils.fs.mount(f"s3a://{access_key}:{encoded_secret_key}@{aws_bucket_name}", f"/mnt/{mount_name}")

# COMMAND ----------

#### Auxiliary functions ####
class Auxiliary_functions():
    """
    Class responsible for auxiliary functions
    """
    def __init__(self):
        pass

    def download_file(self, url, path):
        r = requests.get(url, stream=True)
        if r.status_code == requests.codes.OK:
            with open(path, 'wb') as new_file:
                    for part in r.iter_content(chunk_size=256):
                        new_file.write(part)
        else:
            r.raise_for_status()

    def execute(self, command): 
        process = subprocess.Popen(command, stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True)
        stdout, stderr = process.communicate()
        stdout, stderr = stdout.decode('utf-8'), stderr.decode('utf-8')
        return str(process.returncode), stdout, stderr

    def sink_data_into_deltalake(self, df, path, fields_merge=False, partition_table=False, vacuum=7):
        try:
            dbutils.fs.ls(path)
            
            if fields_merge:
                df.createOrReplaceTempView("upsert_delta")
                spark.sql(f"""
                            MERGE INTO delta.`{path}` AS t 
                            USING upsert_delta AS s
                            ON ({fields_merge})
                            WHEN MATCHED THEN UPDATE SET *
                            WHEN NOT MATCHED THEN INSERT *
                            """)
            else:
                print('Need provide the fields for the merge operation')
                
        except:
        # Write Data Frame
            # This command should be executed in the first execution
            if partition_table:
                df.write.mode("overwrite").format("delta").partitionBy(partition_table).save(path)
            else:
                df.write.mode("overwrite").format("delta").save(path)

            spark.sql(f"""ALTER TABLE delta.`{path}` SET TBLPROPERTIES ('delta.autoOptimize.optimizeWrite' = 'true',
                                                                            'delta.logRetentionDuration' = 'interval {vacuum} days',
                                                                            'delta.deletedFileRetentionDuration' = 'interval {vacuum} days')""")

    def _parse_json_dataframe(self, json_list):
        string_list = [json.dumps(i) for i in json_list]
        rdd = sc.parallelize(string_list)
        return spark.read.json(rdd)

    def download_api_to_dataframe(self, url):
        r = requests.get(url)
        j = r.json()
        return self._parse_json_dataframe(j)
    
    def create_hive_table(self, database, table, s_schema, path, partition_table=False):
        spark.sql(f"CREATE DATABASE IF NOT EXISTS {database}")
        if partition_table:
            spark.sql(f"""
                        CREATE TABLE IF NOT EXISTS {database}.{table}
                        ({s_schema})
                        USING DELTA
                        PARTITIONED BY ({partition_table})
                        LOCATION '{path}'
                       """)
        else:
            spark.sql(f"""
                        CREATE TABLE IF NOT EXISTS {database}.{table}
                        ({s_schema})
                        USING DELTA
                        LOCATION '{path}'
                       """)
    
af = Auxiliary_functions()

# COMMAND ----------

#### Divida ativa data ####
class Divida_ativa():
    """
    Class responsible for the active-debt pipeline
    """
    def __init__(self):
        self._table = "divida-ativa"
        self._url_base = 'http://dadosabertos.pgfn.gov.br/'
        self._zip_name = 'Dados_abertos_Nao_Previdenciario.zip'
        self._url_download = self._url_base + self._zip_name
        self._landing_path = landing_path(self._table)
        self._bronze_path = bronze_path(self._table)
        self._silver_path = silver_path(self._table)
        self._gold_path = gold_path(self._table)
    
    def _clean_landing(self):
        dbutils.fs.rm(self._landing_path, True)
        dbutils.fs.mkdirs(self._landing_path)
    
    def _landing(self):
        af.download_file(self._url_download, f"/tmp/{self._zip_name}")
        
        # if need it its possible investigate the return, output and error values
        ret, out, err = af.execute(f"unzip /tmp/{self._zip_name}")
        
        list_of_csv = [i for i in dbutils.fs.ls("file:/databricks/driver/") if "arquivo_lai_SIDA" in i.path]

        for i in list_of_csv:
            dbutils.fs.mv(i.path, f"{self._landing_path}/{i.name}")
        
        dbutils.fs.rm(f"file:/tmp/{self._zip_name}", True)
        
        print("Data has landing")
        
    def _bronze_layer(self):
        # reading raw data divida-ativa and sink in bronze layer (delta format)
        # reading
        df_divida_ativa_raw = spark.read.csv(self._landing_path, sep=";", header=True, encoding="ISO-8859-1")
        # hashing sensite data
        df_divida_ativa_raw = df_divida_ativa_raw.withColumn('hash_NOME_DEVEDOR', sha1(col('NOME_DEVEDOR')))\
                                                 .withColumn('hash_NUMERO_INSCRICAO', sha1(col('NUMERO_INSCRICAO')))
        # drop sensitive data
        df_divida_ativa_raw = df_divida_ativa_raw.drop(col('CPF_CNPJ'))\
                                                 .drop(col('NOME_DEVEDOR'))\
                                                 .drop(col('NUMERO_INSCRICAO'))
        # adding date_load
        df_divida_ativa_raw = df_divida_ativa_raw.withColumn('date_load', current_date().cast(StringType()))
        # sink
        af.sink_data_into_deltalake(df_divida_ativa_raw,
                                    self._bronze_path,
                                    fields_merge="""
                                    t.hash_NOME_DEVEDOR = s.hash_NOME_DEVEDOR AND
                                    t.hash_NUMERO_INSCRICAO = s.hash_NUMERO_INSCRICAO AND
                                    t.DATA_INSCRICAO = s.DATA_INSCRICAO
                                    """,
                                    partition_table='date_load')
        
        print('Data has sank in Bronze layer')
        
    def _silver_layer(self):
        ## data handling and sink to silver layer
        # reading
        df_divida_ativa_bronze = spark.read.format("delta").load(self._bronze_path)
        # cast correct data type
        df_divida_ativa_bronze = df_divida_ativa_bronze.withColumn('VALOR_CONSOLIDADO', col('VALOR_CONSOLIDADO').cast(DoubleType()))\
                                                       .withColumn('DATA_INSCRICAO', to_date(col('DATA_INSCRICAO'), "dd/MM/yyyy"))

        # drop NA
        df_divida_ativa_bronze = df_divida_ativa_bronze.na.drop()
        # get unique key and time key
        time_key = (when(month(col('DATA_INSCRICAO')).between(1,3),
                         to_date(concat(year(col('DATA_INSCRICAO')), lit('0101')), "yyyyMMdd"))\
                   .when(month(col('DATA_INSCRICAO')).between(4,6),
                         to_date(concat(year(col('DATA_INSCRICAO')), lit('0401')), "yyyyMMdd"))\
                   .when(month(col('DATA_INSCRICAO')).between(7,9),
                         to_date(concat(year(col('DATA_INSCRICAO')), lit('0701')), "yyyyMMdd"))\
                   .when(month(col('DATA_INSCRICAO')).between(10,12),
                         to_date(concat(year(col('DATA_INSCRICAO')), lit('1001')), "yyyyMMdd")))
        
        df_divida_ativa_bronze = df_divida_ativa_bronze.withColumn('time_key', time_key)\
                                                       .withColumn('uuid_key', udf(lambda: str(uuid4()))())
        # reorder columns
        order_columns = df_divida_ativa_bronze.columns
        step_order_columns = []
        step_order_columns.append(order_columns.pop(-1))
        step_order_columns.append(order_columns.pop(-1))
        order_columns = step_order_columns + order_columns
        df_divida_ativa_bronze = df_divida_ativa_bronze.select(order_columns)     
        # sink
        af.sink_data_into_deltalake(df_divida_ativa_bronze,
                                    self._silver_path,
                                    fields_merge="""
                                    t.hash_NOME_DEVEDOR = s.hash_NOME_DEVEDOR AND
                                    t.hash_NUMERO_INSCRICAO = s.hash_NUMERO_INSCRICAO AND
                                    t.DATA_INSCRICAO = s.DATA_INSCRICAO
                                    """,
                                    partition_table='date_load')
        
        print('Data has sank in Silver layer')
    
    def _gold_layer(self):
        ## If necessary, aggregations and joins can be made and placed in the gold layer
        pass

    def start_pipeline(self):
        self._clean_landing()
        self._landing()
        
        self._bronze_layer()
        self._clean_landing()
        
        self._silver_layer()
        
        self._gold_layer()


# COMMAND ----------

#### Banco Central do Brasil data ####
class BCB():
    """
    Class responsible for the Banco Central do Brasil pipeline
    """
    def __init__(self, serie, table):
        self._serie = serie
        self._table = table
        self._url_download = f"http://api.bcb.gov.br/dados/serie/bcdata.sgs.{self._serie}/dados?formato=json"
        self._landing_path = landing_path(self._table)
        self._bronze_path = bronze_path(self._table)
        self._silver_path = silver_path(self._table)
        self._gold_path = gold_path(self._table)
    
    def _bronze_layer(self):
        # reading raw data from api and sink in bronze layer (delta format)
        # reading
        df_serie_raw = af.download_api_to_dataframe(self._url_download)
        # adding date_load
        df_serie_raw = df_serie_raw.withColumn('date_load', current_date().cast(StringType()))
        # sink
        af.sink_data_into_deltalake(df_serie_raw,
                                    self._bronze_path,
                                    fields_merge="""
                                    t.data = s.data
                                    """,
                                    partition_table='date_load')
        
        print('Data has sank in Bronze layer')
    
    def _silver_layer(self):
        ## data handling and sink to silver layer
        df_serie_bronze = spark.read.format("delta").load(self._bronze_path)
        # cast correct data type
        df_serie_bronze = df_serie_bronze.withColumn('valor', col('valor').cast(DoubleType()))\
                                         .withColumn('data', to_date(col('data'), "dd/MM/yyyy"))
        # drop NA
        df_serie_bronze = df_serie_bronze.na.drop()
        # sink
        af.sink_data_into_deltalake(df_serie_bronze,
                                    self._silver_path,
                                    fields_merge="""
                                    t.data = s.data
                                    """,
                                    partition_table='date_load')
        
        print('Data has sank in Silver layer')
    
    def _gold_layer(self):
        ## If necessary, aggregations and joins can be made and placed in the gold layer
        pass
    
    def start_pipeline(self):
        self._bronze_layer()
        
        self._silver_layer()
        
        self._gold_layer()


# COMMAND ----------

#### Pipeline processing ####

# COMMAND ----------

da = Divida_ativa()
da.start_pipeline()

# COMMAND ----------

ccc = BCB('21388', 'credit-condition-corporate')
ccc.start_pipeline()

# COMMAND ----------

ccm = BCB('21395', 'credit-condition-mortgage')
ccm.start_pipeline()

# COMMAND ----------

#### Creating metastore ####

# COMMAND ----------

# divida_ativa_silver
af.create_hive_table('stone_silver',
                  'divida_ativa',
                  '''
                  uuid_key string,
                  time_key date,
                  TIPO_PESSOA string,
                  TIPO_DEVEDOR string,
                  UF_UNIDADE_RESPONSAVEL string,
                  UNIDADE_RESPONSAVEL string,
                  TIPO_SITUACAO_INSCRICAO string,
                  SITUACAO_INSCRICAO string,
                  RECEITA_PRINCIPAL string,
                  DATA_INSCRICAO date,
                  INDICADOR_AJUIZADO string,
                  VALOR_CONSOLIDADO double,
                  hash_NOME_DEVEDOR string,
                  hash_NUMERO_INSCRICAO string,
                  date_load string
                  ''',
                  silver_path('divida-ativa'),
                  'date_load'
                 )

# COMMAND ----------

# credit_condition_corporate_silver
af.create_hive_table('stone_silver',
                  'credit_condition_corporate',
                  '''
                  data date,
                  valor double,
                  date_load string
                  ''',
                  silver_path('credit-condition-corporate'),
                  'date_load'
                 )

# COMMAND ----------

# credit_condition_mortgage_silver
af.create_hive_table('stone_silver',
                  'credit_condition_mortgage',
                  '''
                  data date,
                  valor double,
                  date_load string
                  ''',
                  silver_path('credit-condition-mortgage'),
                  'date_load'
                 )

# COMMAND ----------

#### Selecting from databases with SQL like language ####

# COMMAND ----------

spark.sql("""
SELECT *
FROM stone_silver.divida_ativa
LIMIT 100
""").display()

# COMMAND ----------

spark.sql("""
SELECT *
FROM stone_silver.credit_condition_corporate
LIMIT 100
""").display()

# COMMAND ----------

spark.sql("""
SELECT *
FROM stone_silver.credit_condition_mortgage
LIMIT 100
""").display()
