import uvicorn

from fastapi import FastAPI
from spark.spark import *
from spark.api_body import *


app = FastAPI(
    title="SparkSession",
    description=f'Spark Streaming for ABACUS'
)


@app.get("/")
def root():
    return "Hello World"


@app.post("/prediction/")
async def post_prediction_mem(api_body: PredictionItem):
    stream_data = SPARK.spark_session.createDataFrame([api_body.data])
    new_stream_data = SPARK.add_new_features(stream_data)
    new_stream_data.show()
    # print((new_stream_data.count(), len(new_stream_data.columns)))

    # Parquet
    # Feast
    # BentoML


if __name__ == "__main__":
    # Create SparkSession
    SPARK = SparkModule()

    # Run FastAPI(Spark)
    uvicorn.run(app, host=SPARK.api_contents['api_host'], port=SPARK.api_contents['api_port'])
