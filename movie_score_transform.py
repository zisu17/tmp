from pyspark.sql.functions import col
from infra.jdbc import DataWarehouse, find_data, save_data
from infra.spark_session import get_spark_session
from infra.util import cal_std_day

class MovieScoreTransformer:
    @classmethod
    def transform(cls):
        movie_url_df = find_data(DataWarehouse, 'MOVIE_URL')
        movie_url_df = movie_url_df.drop_duplicates(['MOVIE_CODE'])
        movie_code_list = movie_url_df.select('MOVIE_CODE').rdd.flatMap(lambda x: x).collect()

        path = '/movie_data/score/movie_score_' + \
            movie_code_list[0] + '_' + \
            cal_std_day(0) + '.json'
        mv_score_json = get_spark_session().read.json(path, encoding='UTF-8')