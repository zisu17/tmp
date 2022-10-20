from infra.jdbc import DataMart, DataWarehouse, find_data, save_data
from pyspark.sql.functions import when

class MovieRank:
    @classmethod
    def save(cls):
        boxoffice = find_data(DataWarehouse, 'DAILY_BOXOFFICE')
        movie = find_data(DataMart, 'MOVIE')
        boxoffice = boxoffice.select('MOVIE_CODE','RANK','STD_DATE')
        movie = movie.select('MOVIE_CODE','MOVIE_NAME','OPEN_DATE','HIT_GRADE')

        rank_join = boxoffice.join(movie, on='MOVIE_CODE')
        rank_join.show()

        first_tmp = rank_join.select(rank_join.MOVIE_CODE,rank_join.RANK.alias('FIRST_RANK')).where(rank_join.STD_DATE==rank_join.OPEN_DATE)


        first_rank_yn = first_tmp.withColumn('OPEN_FIRST_RANK_YN', when(first_tmp.FIRST_RANK==1, 'Y')
                                                                    .when(first_tmp.FIRST_RANK!=1, 'N'))
        print('개봉일 영화 랭킹')
        first_rank_yn.show()
        
        second_tmp = rank_join.select(rank_join.MOVIE_CODE,rank_join.RANK.alias('SECOND_RANK')).where(rank_join.STD_DATE==rank_join.OPEN_DATE+7)
        print('개봉일+7일차 영화 랭킹')
        second_tmp.show()

        first_second_join = first_rank_yn.join(second_tmp, on='MOVIE_CODE')
        print('개봉일과 개봉일+7일차가 모두 있는 데이터 출력')
        first_second_join.show()

        rank_yn = first_second_join.withColumn('SEC_WEEK_RANK_DROP', when(first_second_join.FIRST_RANK<first_second_join.SECOND_RANK, 'Y')
                                                                    .when(first_second_join.FIRST_RANK>=first_second_join.SECOND_RANK, 'N'))
        print('개봉일과 개봉일+7일차의 랭킹 비교 하락하면 Y 그렇지 않으면 N')
        rank_yn.show()
        
        rank_yn = rank_yn.select('MOVIE_CODE','OPEN_FIRST_RANK_YN','SEC_WEEK_RANK_DROP')
        print('join에 필요한 데이터만 select')
        rank_yn.show()

        movie_rank_join = movie.join(rank_yn, on='MOVIE_CODE', how='left').drop('OPEN_DATE')
        print('개봉일 랭킹과 개봉일+7일차 랭킹 데이터가 없는 경우엔 null값 나옴')
        movie_rank_join.show()

        movie_rank = movie_rank_join.na.drop('any')
        movie_rank.show()
