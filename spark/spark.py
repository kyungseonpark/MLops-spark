import os
import oyaml
import numpy
from datetime import datetime
from pyspark.sql import DataFrame
from pyspark.sql import SparkSession

# Yaml file path for Features Configuration
SPARK_YAML_PATH = os.getenv('SPARK_YAML_PATH', './volumes/environment.yaml')
now_time = (lambda: datetime.now().strftime('%y%m%d_%H%M%S_%f'))


class SparkModule:
    """
    (kor)
    spark_session: Spark session
    api_contents: Feast API 구동시 필요한 API_HOST, API_PORT, 정보
    bentoml_url: bentoml URL
    feast_url: feast URL
    is_static_first: 통계변수와 파생변수의 YAML 순서에 따라 먼저 생성되는 변수를 구분하기 위함.
    base_sql_list: 통계변수를 위한 base_sql 정보들을 갖고있는 문자열 배열
    derived_functions: 파생변수 생성을 위한 lambda 함수들을 갖고 있는 배열

    (eng)
    spark_session: Spark session
    api_contents: API_HOST, API_PORT needed to run FastAPI
    bentoml_url: bentoml URL
    feast_url: feast URL
    is_static_first: To determine the order of generation of statistical and derivative features.
    base_sql_list: Array of strings containing base SQL information for statistical variables
    derived_functions: Array with lambda functions for generating derivatives
    """
    spark_session: SparkSession
    api_contents: dict
    bentoml_url: str
    feast_url: str
    is_static_first: bool
    base_sql_list: list = list()
    derived_functions: list = list()

    def __init__(self):
        # Initialize Class

        # Read Base YAML
        with open(f'{SPARK_YAML_PATH}', 'r', encoding='utf-8') as base_yaml:
            base_yaml_contents = oyaml.load(base_yaml, Loader=oyaml.FullLoader)

        # yaml version
        yaml_version = base_yaml_contents['version']
        # API_HOST, API_PORT needed to run FastAPI
        self.api_contents = base_yaml_contents['api']
        # info of SparkSession
        self.spark_contents = base_yaml_contents['spark']
        # info of Model, Typically BentoML URL
        model_contents = base_yaml_contents['model']
        # info of Feast
        feast_contents = base_yaml_contents['feast']
        # Features to be generated in real time
        features_contents = base_yaml_contents['features']

        self.__define_spark_session()

        # yaml_version 0.1
        if yaml_version == 0.1:
            self.bentoml_url = model_contents['bentoml_url']
            self.feast_url = feast_contents['feast_url']
            self.is_static_first = True if list(features_contents.keys())[0] == 'statistic_features' else False
            if 'statistic_features' in features_contents.keys():
                self.__make_statistic_sql_list(features_contents['statistic_features'])
            if 'derived_features' in features_contents.keys():
                self.__make_derived_feature_func(features_contents['derived_features'])
        else:
            raise KeyError(f'Version Matching Error: version:({yaml_version}) is not supported.')

    def __define_spark_session(self):
        self.spark_session = (SparkSession.builder.master('local')
                              .appName(self.spark_contents['spark_session_name'])
                              .getOrCreate())

    def __make_statistic_sql_list(self, statistic_features: dict):
        # make statistic SQL List for statistic features
        for parquet_name, parquet_path in statistic_features['base_parquet'].items():
            read_parquet = self.spark_session.read.parquet(parquet_path)
            read_parquet.createOrReplaceTempView(parquet_name)

        self.base_sql_list = list()
        for sql_file_path in statistic_features['sql']:
            with open(sql_file_path, 'r') as sql_file:
                sql = sql_file.readlines()
                self.base_sql_list.append(' '.join(sql))

    def __make_derived_feature_func(self, derived_features: dict):
        # make lambda function for derived features
        self.derived_functions = list()
        for feature_name, contents in derived_features.items():
            option = contents['option']
            features = contents['features']
            dev_func = (lambda x: x.withColumn(feature_name, getattr(numpy, option)(x[fe] for fe in features)))
            self.derived_functions.append(dev_func)

    def add_new_features(self, input_df: DataFrame):
        def __add_statistic_features(df: DataFrame):
            res = df
            for base_sql in self.base_sql_list:
                new_df = self.spark_session.sql(base_sql)
                res = res.crossJoin(new_df)
            return res

        def __add_derived_features(df: DataFrame):
            res = df
            for function in self.derived_functions:
                res = function(res)
            return res

        if self.is_static_first:
            new_stream = __add_statistic_features(input_df)
            new_stream = __add_derived_features(new_stream)
        else:
            new_stream = __add_derived_features(input_df)
            new_stream = __add_statistic_features(new_stream)
        return new_stream
