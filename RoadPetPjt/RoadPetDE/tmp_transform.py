from pyspark.sql.functions import split, col, when, concat
from infra.logger import get_logger
from infra.spark_session import get_spark_session
from infra.util import cal_std_day_yyyymmdd
import pandas as pd
from infra.jdbc import DataWarehouse, find_data, save_data

class ColorTransformer:
    @classmethod
    def transform(cls):
        shelter_df = find_data(DataWarehouse, 'SHELTER')
        roaddog = pd.read_csv('./roaddog_data.csv', encoding='utf-8')
        roaddog = get_spark_session().createDataFrame(roaddog, schema = 'DESERTION_NO string, COLOR string, SEX string, KIND_NM string, AGE string, WEIGHT string, NEUTER_YN string, PROCESS_STATE string, HAPPEN_DT string, HAPPEN_PLACE string, SPECIAL_MARK string, PROFILE string, ST_NM string')

        roaddog = roaddog.join(shelter_df, roaddog.ST_NM == shelter_df.CARE_NM)
        roaddog_select = roaddog.select(
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
        roaddog_select.show()
        save_data(DataWarehouse, roaddog_select, 'ROADDOG_INFO')