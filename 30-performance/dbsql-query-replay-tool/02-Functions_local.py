import uuid
import requests, json
import time
from datetime import datetime, timedelta
from queue import Queue
from threading import Thread
# from pyspark.sql import SparkSession
# from dbruntime.databricks_repl_context import get_context
from pyspark.sql.types import StructType, StructField, StringType
from pyspark.sql.functions import col, min
from pyspark.sql.window import Window
from retry import retry
import logging
from databricks.connect import DatabricksSession
from delta.tables import DeltaTable
from pyspark.sql.functions import from_unixtime, lit
import os

# logging.basicConfig()

# Create a Spark session
# spark = SparkSession.builder.appName("QueryReplayTest").getOrCreate()

# log_filename = f"log_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
# logging.basicConfig(filename=log_filename, level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s', datefmt='%Y-%m-%d %H:%M:%S')

spark = DatabricksSession.builder.remote(
    host="e2-demo-field-eng.cloud.databricks.com",
    cluster_id="1119-114526-n8gv9u9l",
    token="dapi357f4e303ea1eddd81bd9efb01ed6d97"
).getOrCreate()

class QueryReplayTest:
    def __init__(
        self,
        test_name,
        result_catalog,
        result_schema,
        token,
        source_warehouse_id,
        source_start_time,
        source_end_time,
        target_warehouse_size="Small",
        target_warehouse_min_num_clusters=1,
        target_warehouse_max_num_clusters=1,
        target_warehouse_type="PRO",
        target_warehouse_serverless=True,
        target_warehouse_custom_tags=[],
        target_warehouse_channel="CHANNEL_NAME_PREVIEW",
        target_warehouse_auto_stop_mins=3,
        sender_parallelism=250,
        checker_parallelism=50,
        **kwargs,
    ):
        self.token = token
        self.result_catalog = result_catalog
        self.result_schema = result_schema

        self._test_id = kwargs.get("test_id", None)

        if "test_id" in kwargs:
            run = self.show_run.toPandas().to_dict(orient="records")[0]
            self.test_name = run["test_name"]
            self.source_warehouse_id = run["source_warehouse_id"]
            self.source_start_time = run["source_start_time"]
            self.source_end_time = run["source_end_time"]

            self._target_warehouse_name = run["test_name"] + "_" + run["test_id"]
            self._target_warehouse_id = run["target_warehouse_id"]

            self._run_completed = True
        else:
            self.test_name = test_name
            self.source_warehouse_id = source_warehouse_id
            self.source_start_time = source_start_time
            self.source_end_time = source_end_time

            self._target_warehouse_name = None
            self._target_warehouse_id = None

            self._run_completed = False

        self.target_warehouse_size = target_warehouse_size
        self.target_warehouse_max_num_clusters = target_warehouse_max_num_clusters
        self.target_warehouse_min_num_clusters = target_warehouse_min_num_clusters
        self.target_warehouse_type = target_warehouse_type
        self.target_warehouse_serverless = target_warehouse_serverless
        self.target_warehouse_custom_tags = target_warehouse_custom_tags
        self.target_warehouse_channel = target_warehouse_channel
        self.target_warehouse_auto_stop_mins = target_warehouse_auto_stop_mins

        self.sender_parallelism = sender_parallelism
        self.checker_parallelism = checker_parallelism

        self._query_df = None

    @property
    def test_id(self):
        if self._test_id is None:
            self._test_id = str(uuid.uuid4())
        return self._test_id

    @property
    def target_warehouse_name(self):
        if self._target_warehouse_name is None:
            self._target_warehouse_name = self.test_name + "_" + self.test_id
        return self._target_warehouse_name

    @property
    def host(self):
        #return get_context().workspaceUrl
        return "e2-demo-field-eng.cloud.databricks.com"

    @property
    def query_df(self):
        if self._query_df is None:
            self._query_df = self.get_query()
        return self._query_df

    def create_warehouse(self):
        api = f"https://{self.host}/api/2.0/sql/warehouses"
        payload = {
            "name": self.target_warehouse_name,
            "cluster_size": self.target_warehouse_size,
            "min_num_clusters": self.target_warehouse_min_num_clusters,
            "max_num_clusters": self.target_warehouse_max_num_clusters,
            "auto_stop_mins": self.target_warehouse_auto_stop_mins,
            "enable_photon": True,
            "enable_serverless_compute": self.target_warehouse_serverless,
            "warehouse_type": self.target_warehouse_type,
            "tags": {"custom_tags": self.target_warehouse_custom_tags},
            "channel": {"name": self.target_warehouse_channel},
        }
        headers = {
            "Authorization": f"Bearer {self.token}",
            "Content-Type": "application/json",
        }

        try:
            response = requests.post(api, headers=headers, data=json.dumps(payload))
            response = json.loads(response.text)["id"]
        except requests.exceptions.RequestException as e:
            print(f"An error occurred: {e}")

        return response

    def start_warehouse(self):
        api = f"https://{self.host}/api/2.0/sql/warehouses/{self.target_warehouse_id}/start"
        headers = {
            "Authorization": f"Bearer {self.token}",
            "Content-Type": "application/json",
        }

        try:
            response = requests.post(api, headers=headers)
            response = json.loads(response.text)["id"]
        except requests.exceptions.RequestException as e:
            print(f"An error occurred: {e}")

    @property
    def target_warehouse_id(self):
        if self._target_warehouse_id is None:
            self._target_warehouse_id = self.create_warehouse()
        return self._target_warehouse_id

    def get_query(self):
        df = spark.sql(
            f"""
            SELECT unix_timestamp(start_time) as start_time, statement_id, statement_text
            FROM system.query.history
            WHERE statement_type IN ('SELECT') --WHAT OTHER TYPE SHOULD WE CARE ABOUT?
              AND warehouse_id = "{self.source_warehouse_id}"
              AND error_message is null
              AND start_time BETWEEN "{self.source_start_time}" AND "{self.source_end_time}"
            """
        )
        return df

    @retry(delay=2, jitter=2)
    def send_query(self, statement):
        api = f"https://{self.host}/api/2.0/sql/statements/"
        payload = {
            "warehouse_id": self.target_warehouse_id,
            "statement": statement,
            "wait_timeout": "0s",
            "disposition": "EXTERNAL_LINKS",
        }
        headers = {
            "Authorization": f"Bearer {self.token}",
            "Content-Type": "application/json",
        }

        # try:
        response = requests.post(api, headers=headers, data=json.dumps(payload))
        response = json.loads(response.text)["statement_id"]
        # except requests.exceptions.RequestException as e:
        #     print(f"An error occurred: {e}")

        return response

    def wait_and_send_query(self, wait_time, statement):
        if wait_time > 0:
            time.sleep(wait_time)
        elif round(wait_time) < 0:
            print(f"lagging behind by {abs(wait_time)} second")
        res = self.send_query(statement)
        return res

    @retry(delay=2, jitter=2)
    def check_status(self, statement_id):
        api = f"https://{self.host}/api/2.0/sql/statements/{statement_id}"
        headers = {
            "Authorization": f"Bearer {self.token}",
            "Content-Type": "application/json",
        }

        # try:
        response = requests.get(api, headers=headers)
        response = response.json()["status"]["state"]
        # except requests.exceptions.RequestException as e:
        #     print(f"An error occurred: {e}")

        return response

    def replay_queries(self, queries):
        def query_sender(q1, q2):
            while True:
                start_time, offset, qid, q, d = q1.get()
                tid = self.wait_and_send_query(
                    offset - (datetime.now() - start_time).seconds, q
                )
                #print(f"{datetime.now()} - sending query - {tid}")
                #logging.info(f"{datetime.now()} - sending query - {tid}")
                q2.put((qid, tid, d, datetime.now()+timedelta(seconds=d*0.8)))
                q1.task_done()

        def query_checker(q2, q3):
            while True:
                qid, tid, d, dt = q2.get()
                if dt > datetime.now():
                    q2.put((qid, tid, d, dt))
                else:
                    status = self.check_status(tid)
                    # print(f"{datetime.now()} - fetching result - {tid} - {status}")
                    # logging.info(f"{datetime.now()} - fetching result - {tid} - {status}")
                    if status not in ["SUCCEEDED", "FAILED", "CANCELED", "CLOSED"]:
                        q2.put((qid, tid, d, dt+timedelta(seconds=d/2)))
                    else:
                        q3.put((qid, tid))
                    q2.task_done()

        input_q = Queue(maxsize=0)
        submitted_q = Queue(maxsize=0)
        completed_q = Queue(maxsize=0)

        for i in range(self.sender_parallelism):
            worker = Thread(target=query_sender, args=(input_q, submitted_q))
            worker.daemon = True
            worker.start()

        for i in range(self.checker_parallelism):
            worker = Thread(target=query_checker, args=(submitted_q, completed_q))
            worker.daemon = True
            worker.start()

        queries.sort(key=lambda x: x[0])
        first_query_start_time = queries[0][0]
        normalized_queries = [
            (datetime.now(), t - first_query_start_time, i, q, d)
            for t, i, q, d in queries
        ]

        for q in normalized_queries:
            input_q.put(q)

        while completed_q.qsize() < len(queries):
            print(
                f"In Progress - {input_q.qsize()} queries to be sent - {submitted_q.qsize()} queries to be fetched - {completed_q.qsize()} / {len(queries)} queries completed"
            )
            # logging.info(f"In Progress - {input_q.qsize()} queries to be sent - {submitted_q.qsize()} queries to be fetched - {completed_q.qsize()} / {len(queries)} queries completed")
            time.sleep(30)

        return list(completed_q.queue)

    def init_schema(self, overwrite=False):
        try:
            spark.sql(f"USE CATALOG {self.result_catalog}")
            if overwrite:
                spark.sql(
                    f"DROP TABLE IF EXISTS {self.result_schema}.query_replay_test_run"
                )
                spark.sql(
                    f"DROP TABLE IF EXISTS {self.result_schema}.query_replay_test_run_details"
                )

            spark.sql(f"CREATE SCHEMA IF NOT EXISTS {self.result_schema}")
            spark.sql(f"USE SCHEMA {self.result_schema}")

            spark.sql(
                """
                CREATE TABLE IF NOT EXISTS query_replay_test_run (
                    test_name STRING,
                    test_start_time TIMESTAMP,
                    test_id STRING,
                    source_warehouse_id STRING,
                    source_start_time TIMESTAMP,
                    source_end_time TIMESTAMP,
                    target_warehouse_id STRING,
                    query_count STRING)
                """
            )

            spark.sql(
                """
                CREATE TABLE IF NOT EXISTS query_replay_test_run_details (
                test_id STRING,
                source_warehouse_id STRING,
                source_statement_id STRING,
                target_warehouse_id STRING,
                target_statement_id STRING)
                """
            )

        except requests.exceptions.RequestException as e:
            print(f"An error occurred: {e}")

        return spark.sql(f"SHOW TABLES IN {self.result_catalog}.{self.result_schema}")

    def log_run(self, query_df):
        insert = spark.sql(
            f"""
        INSERT INTO query_replay_test_run VALUES(
        '{self.test_name}',
        '{datetime.now().strftime("%Y-%m-%d %H:%M:%S")}',
        '{self.test_id}',
        '{self.source_warehouse_id}',
        '{self.source_start_time}',
        '{self.source_end_time}',
        '{self.target_warehouse_id}',
        {query_df.count()}
        );
        """
        )
        return insert

    def log_run_details(self, results):
        result_df = spark.createDataFrame(
            list(
                (
                    self.test_id,
                    self.source_warehouse_id,
                    r[0],
                    self.target_warehouse_id,
                    r[1],
                )
                for r in results
            ),
            StructType(
                [
                    StructField("test_id", StringType(), True),
                    StructField("source_warehouse_id", StringType(), True),
                    StructField("source_statement_id", StringType(), True),
                    StructField("target_warehouse_id", StringType(), True),
                    StructField("target_statement_id", StringType(), True),
                ]
            ),
        )

        insert = result_df.write.insertInto(
            f"{self.result_catalog}.{self.result_schema}.query_replay_test_run_details"
        )

        return insert

    @property
    def show_run(self):
        return spark.sql(
            f"select * from {self.result_catalog}.{self.result_schema}.query_replay_test_run where test_id = '{self.test_id}'"
        )

    @property
    def show_run_details(self):
        return spark.sql(
            f"select * from {self.result_catalog}.{self.result_schema}.query_replay_test_run_details where test_id = '{self.test_id}'"
        )

    def run(self, overwrite_schema=False):
        if self._run_completed:
            print(f"run already completed - test id: {self.test_id}")
        else:
            print(f"starting run - test id: {self.test_id}")
            self.init_schema(overwrite_schema)
            self.log_run(self.query_df)
            # make sure the running cluster needs to be on all the time...
            queries = self.query_df.collect()
            results = self.replay_queries(queries)
            self.log_run_details(results)
            self._run_completed = True
            print(f"run completed - test id: {self.test_id}")
            self.store_query_history(self._target_warehouse_id)
            print(f"query history stored - warehouse id: {self._target_warehouse_id}")

        return self.test_id

    @property
    def query_results(self):
        run_details = self.show_run_details
        query_history = spark.read.table("system.query.history")

        comparison = (
            run_details.alias("r")
            .join(
                query_history.alias("s"),
                (col("r.source_warehouse_id") == col("s.warehouse_id"))
                & (col("r.source_statement_id") == col("s.statement_id")),
                "leftouter",
            )
            .join(
                query_history.alias("t"),
                (col("r.target_warehouse_id") == col("t.warehouse_id"))
                & (col("r.target_statement_id") == col("t.statement_id")),
                "leftouter",
            )
            .select(
                col("r.*"),
                col("s.start_time").alias("source_start_time"),
                col("s.total_duration_ms").alias("source_execution_time"),
                (
                    col("s.start_time")
                    - min(col("s.start_time")).over(
                        Window.partitionBy("r.source_warehouse_id")
                    )
                ).alias("source_offset"),
                col("t.start_time").alias("target_start_time"),
                col("t.total_duration_ms").alias("target_execution_time"),
                (
                    col("t.start_time")
                    - min(col("t.start_time")).over(
                        Window.partitionBy("r.target_warehouse_id")
                    )
                ).alias("target_offset"),
            )
        ).withColumns(
            {
                "offset_diff": (col("source_offset") - col("target_offset")).cast(
                    "long"
                ),
                "execution_diff": col("source_execution_time")
                - col("target_execution_time"),
            }
        )

        return comparison
    
    def store_query_history(self, warehouse_id, start_time_ms=None, end_time_ms=None):
        #start_date = datetime.now() - timedelta(hours=NUM_HOURS_TO_UPDATE)
        # start_time_ms = start_date.timestamp() * 1000
        # end_time_ms = datetime.now().timestamp() * 1000
        MAX_RESULTS_PER_PAGE = 1000
        MAX_PAGES_PER_RUN = 1000

        CATALOG_NAME = "perf_test"
        DATABASE_NAME = "query_history"
        ENDPOINTS_TABLE_NAME = "endpoints"
        QUERIES_TABLE_NAME = "queries"

        # Set this to True if the queries Delta table doesn't exist yet.
        # create perf_test.query_history beforehand if not exists
        CREATE_TABLE = False

        HOST = 'https://e2-demo-field-eng.cloud.databricks.com'
        auth_header = {"Authorization" : "Bearer " + self.token}

        warehouse_ids = [warehouse_id]
        next_page_token = None
        has_next_page = True
        pages_fetched = 0

        # uncomment below when you want to update DeltaTable schema (for example when new API fields are added)
        # however it can potentially be a breaking change for the queries/dashboards/alerts which are using this data.
        # spark.sql("SET spark.databricks.delta.schema.autoMerge.enabled = true") 

        while (has_next_page and pages_fetched < MAX_PAGES_PER_RUN):
            print("Starting to fetch page " + str(pages_fetched))
            pages_fetched += 1
            if next_page_token:
                # Can not set filters after the first page
                request_parameters = {
                "max_results": MAX_RESULTS_PER_PAGE,
                "page_token": next_page_token
                }
            else:
                if start_time_ms is not None and end_time_ms is not None:
                    request_parameters = {
                        "max_results": MAX_RESULTS_PER_PAGE,
                        "filter_by": {"query_start_time_range": {"start_time_ms": start_time_ms, "end_time_ms": end_time_ms},
                                    "warehouse_ids": warehouse_ids}
                    }
                else:
                    request_parameters = {
                        "max_results": MAX_RESULTS_PER_PAGE,
                        "filter_by": {"warehouse_ids": warehouse_ids}
                    }

            print ("Request parameters: " + str(request_parameters))
            
            response = requests.get(HOST + "/api/2.0/sql/history/queries", headers=auth_header, json=request_parameters)
            if response.status_code != 200:
                raise Exception(response.text)
            response_json = response.json()
            next_page_token = response_json["next_page_token"]
            has_next_page = response_json["has_next_page"]
            
            queries_json = response_json["res"]

            # A quirk in Python's and Spark's handling of JSON booleans requires us to converting True and False to true and false
            for query in queries_json:
                query["is_final"] = str(query["is_final"]).lower()
                query["canSubscribeToLiveQuery"] = str(query["canSubscribeToLiveQuery"]).lower()
            query_results = spark.read.json(sc.parallelize(queries_json))
            
            query_results.printSchema()
            # For querying convience, add columns with the time in seconds instead of milliseconds
            query_results_clean = query_results \
                .withColumn("query_start_time", from_unixtime(query_results.query_start_time_ms / 1000)) \
                .withColumn("query_end_time", from_unixtime(query_results.query_end_time_ms / 1000))
            
            # The error_message column is not present in the REST API response when none of the queries failed.
            # In that case we add it as an empty column, since otherwise the Delta merge would fail in schema
            # validation
            if "error_message" not in query_results_clean.columns:
                query_results_clean = query_results_clean.withColumn("error_message", lit(""))
            
            query_results_clean.createOrReplaceTempView("newQueryResults")

            if CREATE_TABLE:
                # TODO: Probably makes sense to partition and/or Z-ORDER this table.
                # query_results_clean.write.format("delta").saveAsTable(CATALOG_NAME + "." + DATABASE_NAME + "." + QUERIES_TABLE_NAME) 
                
                spark.sql(f"""
                    CREATE TABLE IF NOT EXISTS {CATALOG_NAME}.{DATABASE_NAME}.{QUERIES_TABLE_NAME}
                    USING delta
                    AS SELECT * FROM newQueryResults
                """)
            else:
                # Merge this page of results into the Delta table.
                spark.sql(f"""
                            INSERT INTO {CATALOG_NAME}.{DATABASE_NAME}.{QUERIES_TABLE_NAME}
                            SELECT * FROM newQueryResults
                """)
                
        
