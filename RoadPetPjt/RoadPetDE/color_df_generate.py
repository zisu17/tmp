import json
from pyspark.sql.functions import split, col
from infra.logger import get_logger
from infra.spark_session import get_spark_session
from infra.util import cal_std_day_yyyymmdd
import pandas as pd


class ColorDictGenerator:
    @classmethod
    def generate(cls):
        color_pd = pd.DataFrame(columns=['DESERTION_NO','COLOR','KIND_NM','AGE','WEIGHT' \
            ,'NEUTER_YN','PROCESS_STATE','HAPPEN_DT','HAPPEN_PLACE','SPECIAL_MARK','PROFILE'])

        color_df = get_spark_session().createDataFrame(color_pd \
                            , schema = 'DESERTION_NO string, COLOR string, KIND_NM string, AGE string \
                                        , WEIGHT string, NEUTER_YN string, PROCESS_STATE string, HAPPEN_DT string \
                                        , HAPPEN_PLACE string, SPECIAL_MARK string, PROFILE string')

        for i in range(1, 365):
            try:
                path = '/roadpet/detail/road_dog_' + cal_std_day_yyyymmdd(i) + '.json'
                road_dog_json = get_spark_session().read.option("multiline","true").option("ignoreLeadingWhiteSpace","true").json(path, encoding='UTF-8').first()
                tmp = get_spark_session().createDataFrame(road_dog_json['response']['body']['items']['item'])
                road_dog_df = tmp.withColumn('KIND_NM', split(tmp['kindCd'], ' ', limit=2).getItem(1)) \
                                        .withColumn('AGE', split(tmp['age'], '\\(', limit=2).getItem(0)) \
                                        .withColumn('WEIGHT', split(tmp['weight'], '\\(', limit=2).getItem(0))

                road_dog_select = road_dog_df.select(
                    col('desertionNo').alias('DESERTION_NO')
                    ,col('colorCd').alias('COLOR')
                    ,col('KIND_NM')
                    ,col('AGE')
                    ,col('WEIGHT')
                    ,col('neuterYn').alias('NEUTER_YN')
                    ,col('processState').alias('PROCESS_STATE')
                    ,col('happenDt').alias('HAPPEN_DT')
                    ,col('happenPlace').alias('HAPPEN_PLACE')
                    ,col('specialMark').alias('SPECIAL_MARK')
                    ,col('filename').alias('PROFILE'))

                color_df = color_df.unionByName(road_dog_select)

            except :
                pass

        color_df.toPandas().to_csv('./roaddog_data2.csv',encoding = 'utf-8', index = False)
        # color_df.write.mode("overwrite").format("csv").option("header", "true").save("/roadpet/roadpet_data.csv")