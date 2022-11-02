import json
from pyspark.sql.functions import split, col, when, concat
from infra.logger import get_logger
from infra.spark_session import get_spark_session
from infra.util import cal_std_day_yyyymmdd
import pandas as pd


class RoadDogGenerator:
    @classmethod
    def generate(cls):
        road_dog_pd = pd.DataFrame(columns=['DESERTION_NO','COLOR','SEX','KIND_NM','AGE','WEIGHT' \
            ,'NEUTER_YN','PROCESS_STATE','HAPPEN_DT','HAPPEN_PLACE','SPECIAL_MARK','PROFILE','ST_NM'])

        road_dog = get_spark_session().createDataFrame(road_dog_pd \
                            , schema = 'DESERTION_NO string, COLOR string, SEX string, KIND_NM string, AGE string \
                                        , WEIGHT string, NEUTER_YN string, PROCESS_STATE string, HAPPEN_DT string \
                                        , HAPPEN_PLACE string, SPECIAL_MARK string, PROFILE string, ST_NM string')

        for i in range(200, 366):
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
                    ,col('sexCd').alias('SEX')
                    ,col('KIND_NM')
                    ,col('AGE')
                    ,col('WEIGHT')
                    ,col('neuterYn').alias('NEUTER_YN')
                    ,col('processState').alias('PROCESS_STATE')
                    ,col('happenDt').alias('HAPPEN_DT')
                    ,col('happenPlace').alias('HAPPEN_PLACE')
                    ,col('specialMark').alias('SPECIAL_MARK')
                    ,col('filename').alias('PROFILE')
                    ,col('careNm').alias('ST_NM'))

                road_dog = road_dog.unionByName(road_dog_select)

            except :
                pass

        road_dog = road_dog.withColumn("white", when(col('COLOR').rlike('^.*흰|백|(크림)|(화이트)|(아이보리)|(하양)|(하얀)|(백구)|희|(white).*'),"흰").otherwise(""))\
                            .withColumn("gold", when(col('COLOR').rlike('^.*(노랑)|(노란)|(누런)|(누렁)|황|금|(골드)|(베이지).*'), "금").otherwise(""))\
                            .withColumn("brown", when(col('COLOR').rlike('^.*갈|(브라운)|밤|(초코)|(고동)|(커피)|탄|(쵸코).*'), "갈").otherwise(""))\
                            .withColumn("grey", when(col('COLOR').rlike('^.*회|(실버)|(그레이).*'), "회").otherwise(""))\
                            .withColumn("black", when(col('COLOR').rlike('^.*검|흑|(블랙).*'), "검").otherwise(""))\
                            .withColumn("COLOR", concat('white','gold','brown','grey','black'))

        road_dog = road_dog.select(
                col('DESERTION_NO')
                ,col('COLOR')
                ,col('SEX')
                ,col('KIND_NM')
                ,col('AGE')
                ,col('WEIGHT')
                ,col('NEUTER_YN')
                ,col('PROCESS_STATE')
                ,col('HAPPEN_DT')
                ,col('HAPPEN_PLACE')
                ,col('SPECIAL_MARK')
                ,col('PROFILE')
                ,col('ST_NM'))

        road_dog.toPandas().to_csv('./roaddog_data2.csv',encoding = 'utf-8', index = False)
        # color_df.write.mode("overwrite").format("csv").option("header", "true").save("/roadpet/roadpet_data.csv")