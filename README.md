# ABACUS Spark (SDP: Streaming Data Processor)

## main.py

- FastAPI가 실행되는 main 코드
- SparkSession을 포함한 Spark 제어에 필요한 관련 정보를 품고 있는 Spark Module을 생성&선언 한다.

## SPARK YAML

- docker-compose로 Container를 실행할때 환경변수로 설정해줘야함.
- SPARK_YAML_PATH = os.getenv('SPARK_YAML_PATH', './volumes/environment.yaml')

### Spark YAML의 구조

```yaml
version: 0.1
api:
  api_host: 0.0.0.0
  api_port: 22222
  data_source:
    option: restapi
  data_source:
    option: kafka
    url: 0.0.0.0:9998
    topic: model_1_topic
    pk: CRD_SUM
  data_source:
    option: redis
    topic: model_1_topic
    pk: CRD_SUM
spark:
  spark_session_name: spark-session
model:
  bentoml_url: http://0.0.0.0:22223
feast:
  feast_url: http://0.0.0.0:22224
features:
  statistic_features:
    base_parquet:
      OVS_BASE: ./volumes/data/OVS_concat_1911.parquet
      OVS_BASE_2: ./volumes/data/OVS_train_test.parquet
    sql:
      - ./volumes/sql/test1.txt
      - ./volumes/sql/test2.sql
      - ./volumes/sql/test3.sql
  derived_features:
    CRD_SUM:
      option: sum
      features: ["CRD_LAST_GAP", "CRD_LASTB_GAP", "CRD_LAST_GAP_BIZ"]
      data_type: double
```

#### version

```yaml
# yaml 파일의 버전
version: 0.1
```

#### api

```yaml
# FastAPI와 Data Source에 대한 정보
api:
	# FastAPI의 Host
  api_host: 0.0.0.0
 	# FastAPI의 port
  api_port: 22222
  #
  # Data Source
	# RestAPI, Kafka, Redis 지원 (택 1)
  data_source:
    option: restapi
  data_source:
    option: kafka
    url: 0.0.0.0:9998
    topic: model_1_topic
    pk: CRD_SUM
  data_source:
    option: redis
    topic: model_1_topic
    pk: CRD_SUM
```

#### spark

```yaml
# Spark에 대한 설정 정보
spark:
  spark_session_name: spark-session
```

#### model

```yaml
# BentoML 정보
# TBD: list로 처리해야함.
model:
  bentoml_url: http://0.0.0.0:22223
```

#### feast

```yaml
# Feast 정보
feast:
  feast_url: http://0.0.0.0:22224
```

#### features

```yaml
# 실시간 생성 변수 정보
features:
  statistic_features:
    base_parquet:
      OVS_BASE: ./volumes/data/OVS_concat_1911.parquet
      OVS_BASE_2: ./volumes/data/OVS_train_test.parquet
    sql:
      - ./volumes/sql/test1.txt
      - ./volumes/sql/test2.sql
      - ./volumes/sql/test3.sql
  derived_features:
    CRD_SUM:
      option: sum
      features: ["CRD_LAST_GAP", "CRD_LASTB_GAP", "CRD_LAST_GAP_BIZ"]
      data_type: double
```

