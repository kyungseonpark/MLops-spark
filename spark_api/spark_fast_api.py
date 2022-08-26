from fastapi import FastAPI

spark = FastAPI(
    title="SparkSession",
    description=f'Spark Streaming for ABACUS'
)

@spark.post("/open-spark-session/")
def post_open_spark_session(api_body):
    """
    세션은 Express에서 구상한 Module에 matching해야할 것 같음.
    :param api_body:
    project_id: int
    service_id: int
    module_id: int
    :return:
    """
    pass


@spark.post("/input-data/")
def post_input_data(api_body):
    """
    1. user가 데이터 넣음(Raw Data)
        1-1. spark 세션으로 데이터 넣음.
        1-2. Feast Offline Store로 Push.
    2. spark에서 나온 데이터(Processed Data)
        2-1. BentoML로 prediction 함.
        2-2. Feast Offline Store로 push.
    3. BentoML - prediction 받은 값을 Return
    :param api_body:
    project_id: int
    service_id: int
    module_id: int
    timestamp_column: str
    dataset_feastures: dict
    :return:
    prediction: dict
    """
    pass


@spark.post("/tmp/")
def tmp(api_body):
    pass
