from pyspark.sql.functions import split, col, when, concat
from infra.logger import get_logger
from infra.spark_session import get_spark_session
from infra.util import cal_std_day_yyyymmdd
import pandas as pd
from infra.jdbc import DataWarehouse, find_data
from konlpy.tag import Kkma
from konlpy.utils import pprint

class ColorTransformer:
    @classmethod
    def transform(cls):
        shelter_df = find_data(DataWarehouse, 'SHELTER')

        for i in range(9, 10):
            path = '/roadpet/detail/road_dog_' + cal_std_day_yyyymmdd(i) + '.json'
            road_dog_json = get_spark_session().read.option("multiline","true").option("ignoreLeadingWhiteSpace","true").json(path, encoding='UTF-8').first()
            tmp = get_spark_session().createDataFrame(road_dog_json['response']['body']['items']['item'])
            road_dog_tmp = tmp.withColumn('KIND_NM', split(tmp['kindCd'], ' ', limit=2).getItem(1)) \
                                    .withColumn('AGE', split(tmp['age'], '\\(', limit=2).getItem(0)) \
                                    .withColumn('WEIGHT', split(tmp['weight'], '\\(', limit=2).getItem(0))

            # road_dog_select = road_dog_tmp.select(
            #     col('desertionNo').alias('DESERTION_NO')
            #     ,col('KIND_NM')
            #     ,col('AGE')
            #     ,col('WEIGHT')
            #     ,col('neuterYn').alias('NEUTER_YN')
            #     ,col('processState').alias('PROCESS_STATE')
            #     ,col('happenDt').alias('HAPPEN_DT')
            #     ,col('happenPlace').alias('HAPPEN_PLACE')
            #     ,col('specialMark').alias('SPECIAL_MARK')
            #     ,col('filename').alias('PROFILE')
            #     ,col('colorCd').alias('COLOR')
            #     ,col('careTel').alias('CARETEL'))
            
            road_dog_tmp = road_dog_tmp.withColumn("white", when(col('colorCd').rlike('^.*흰|백|(크림)|(화이트)|(아이보리)|(하양)|(하얀)|(백구)|희|(white).*'),"흰").otherwise(""))\
                                        .withColumn("gold", when(col('colorCd').rlike('^.*(노랑)|(노란)|(누런)|(누렁)|황|금|(골드)|(베이지).*'), "금").otherwise(""))\
                                        .withColumn("brown", when(col('colorCd').rlike('^.*갈|(브라운)|밤|(초코)|(고동)|(커피)|탄|(쵸코).*'), "갈").otherwise(""))\
                                        .withColumn("grey", when(col('colorCd').rlike('^.*회|(실버)|(그레이).*'), "회").otherwise(""))\
                                        .withColumn("black", when(col('colorCd').rlike('^.*검|흑|(블랙).*'), "검").otherwise(""))\
                                        .withColumn("COLOR", concat('white','gold','brown','grey','black'))

            # road_dog_tmp.select(col('colorCd'),col('COLOR')).show()
            mark = road_dog_tmp.select(col('specialMark')).collect()
            SPECIAL_MARK = []
            kkma=Kkma()
            for i in range(len(mark)):
                if mark[i]['specialMark'].replace(' ','') == '':
                    SPECIAL_MARK.append('-')
                else :
                    pos_character = kkma.pos(mark[i]['specialMark'])

                    word_list = []

                    for pos in pos_character:
                        if pos[1] in ['NR','NNM','XR','NNG','NNP', 'VV', 'VA', 'MAG']:
                            word_list.append(pos[0])
                            
                    word = " ".join(word_list)

                    SPECIAL_MARK.append(word)

            mark_df = get_spark_session.createDataFrame(SPECIAL_MARK)
            mark_df.show()
