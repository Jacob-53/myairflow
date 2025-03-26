from pyspark.sql import SparkSession
import sys
import logging

spark = SparkSession.builder.appName("movie_meta").getOrCreate()

# load_dt = sys.argv[1]
# print("*" * 100)
# print("load_dt=", load_dt)
# print("*" * 100)

# movie_df = spark.read.parquet('/Users/jacob/data/zep_movie.parquet')
# movie_df.createOrReplaceTempView("temp_movie")

# df1 = spark.sql("SELECT * FROM temp_movie WHERE multiMovieYn IS NULL")
# df2 = spark.sql("SELECT * FROM temp_movie WHERE repNationCd IS NULL")
# df1.createOrReplaceTempView("multi_null")
# df2.createOrReplaceTempView("nation_null")

# sql = """
# SELECT
#   COALESCE(m.movieCd, n.movieCd) AS movieCd,
#   COALESCE(m.movieNm, n.movieNm) AS movieNm,
#   m.multiMovieYn,
#   n.repNationCd
# FROM (
#   -- multi 기준 정제 쿼리
#   WITH null_case AS (
#     SELECT movieCd, movieNm, multiMovieYn, repNationCd
#     FROM nation_null
#     WHERE multiMovieYn IS NULL
#   ),
#   non_null_case AS (
#     SELECT movieCd, movieNm, multiMovieYn, repNationCd
#     FROM nation_null
#     WHERE multiMovieYn IS NOT NULL
#   ),
#   clean_null_case AS (
#     SELECT n.*
#     FROM null_case n
#     LEFT JOIN non_null_case nn ON n.movieCd = nn.movieCd
#     WHERE nn.movieCd IS NULL
#   )
#   SELECT * FROM non_null_case
#   UNION
#   SELECT * FROM clean_null_case
# ) m
# FULL OUTER JOIN (
#   -- nation 기준 정제 쿼리
#   WITH null_case AS (
#     SELECT movieCd, movieNm, multiMovieYn, repNationCd
#     FROM multi_null
#     WHERE repNationCd IS NULL
#   ),
#   non_null_case AS (
#     SELECT movieCd, movieNm, multiMovieYn, repNationCd
#     FROM multi_null
#     WHERE repNationCd IS NOT NULL
#   ),
#   clean_null_case AS (
#     SELECT n.*
#     FROM null_case n
#     LEFT JOIN non_null_case nn ON n.movieCd = nn.movieCd
#     WHERE nn.movieCd IS NULL
#   )
#   SELECT * FROM non_null_case
#   UNION
#   SELECT * FROM clean_null_case
# ) n
# ON m.movieCd = n.movieCd
# """

# final_df= spark.sql(sql)
# final_df.createOrReplaceTempView("temp_join_unique")
# final_df.count()



# TODO 나머지 코드 넣기
# final_df.show()
# print("count=", final_df.count())

exit_code = 0

try:
    #{{ds_nodash}} append $META_PATH
    if len(sys.argv) != 4:
      raise ValueError("필수 인자가 누락 되었습니다")

    raw_path, mode, meta_path = sys.argv[1:4]
    # load_dt = sys.argv[1]
    # mode = sys.argv[2]
    # meta_path = sys.argv[3]
    #/Users/jacob/data/movie_after/dailyboxoffice/dt={load_dt}
    raw_df = spark.read.parquet(raw_path)
    raw_df.show()
    raw_df.select("movieCd", "multiMovieYn", "repNationCd").show()

except Exception as e:
    logging.error(f"오류:{str(e)}")
    exit_code = 1
finally:
    spark.stop()
    sys.exit(exit_code)