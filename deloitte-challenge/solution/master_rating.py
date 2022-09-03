import pandas as pd
from pyspark.sql import SparkSession
from pyspark.context import SparkContext
from pyspark.sql.types import *
from pyspark.sql.functions import *

sheets = ['homologacion_pais','homologacion_rating','rating_empresa','rating_soberano']

sc = SparkSession.builder.appName("PysparkDeloitte")\
.config ("spark.sql.shuffle.partitions", "50")\
.config("spark.driver.maxResultSize","5g")\
.config ("spark.sql.execution.arrow.enabled", "true")\
.getOrCreate()

#STEP 0: GETTING DATA
#CSV FILES# 
df_hp = sc.read.csv('data/'+sheets[0]+'.csv', sep='|', header=True)
df_hr = sc.read.csv('data/'+sheets[1]+'.csv', sep='|', header=True)
df_re = sc.read.csv('data/'+sheets[2]+'.csv', sep='|', header=True)
df_rs = sc.read.csv('data/'+sheets[3]+'.csv', sep='|', header=True)

df_hp.createOrReplaceTempView("H_PAIS")
df_hr.createOrReplaceTempView("H_RATING")
df_re.createOrReplaceTempView("R_EMPRESA")
df_rs.createOrReplaceTempView("R_SOBERANO")

#STEP 1: GETTING pais FROM H_PAIS (homologacion_pais.csv)
df_master = sc.sql('SELECT rut, dv, nombre, RE.pais_bbg, UPPER(HP.pais) as pais, mdy, sp, fitch \
                    FROM R_EMPRESA RE \
                    LEFT JOIN H_PAIS HP \
                        ON UPPER(RE.pais_bbg) == UPPER(HP.pais_bbg)')

df_master.createOrReplaceTempView("MASTER")

#STEP 2: SPLITING UP THE DATA
df_master_noagg = sc.sql("SELECT rut, dv, nombre, pais_bbg, pais, mdy as rating, 'MDY' as agencia FROM MASTER \
                         UNION ALL \
                         SELECT rut, dv, nombre, pais_bbg, pais, sp as rating, 'SP' as agencia FROM MASTER \
                         UNION ALL \
                         SELECT rut, dv, nombre, pais_bbg, pais, fitch as rating, 'FITCH' as agencia FROM MASTER")

df_master_noagg.createOrReplaceTempView("MASTER_NOAGG")

#STEP 3: GETTING rating_norma
df_master_noagg_on = sc.sql('SELECT M.*, HR.orden_norma, HR.rating_norma \
                            FROM MASTER_NOAGG M \
                            LEFT JOIN H_RATING HR \
                                ON M.RATING=HR.RATING AND M.AGENCIA=HR.AGENCIA_HOMOL')

df_master_noagg_on.createOrReplaceTempView("MASTER_NOAGG_ON")

#STEP 4: USING rating_norma TO RANK
df_master_noagg_on_ranked = sc.sql('SELECT *, RANK() OVER(PARTITION BY rut, dv, nombre, pais_bbg, pais ORDER BY orden_norma DESC) as rank \
                                    FROM MASTER_NOAGG_ON')
df_master_noagg_on_ranked.createOrReplaceTempView("MASTER_NOAGG_ON_RANKED")

#STEP 5: SELECTING rank = 1 AND DROPING DUPLICATES
df_master2 = sc.sql('SELECT rut, dv, nombre, pais_bbg, pais, rating_norma as rating_empresa \
                     FROM MASTER_NOAGG_ON_RANKED \
                     WHERE rank = 1').dropDuplicates()

df_master2.createOrReplaceTempView("MASTER2")

#STEP 6: CLEANING rating_soberano.csv
df_rs_wu = sc.sql('SELECT pais_bbg, REPLACE(sp,"u","") as SP, REPLACE(fitch,"u","") as FITCH, REPLACE(mdy,"u","") as MDY \
                   FROM R_SOBERANO')

df_rs_wu.createOrReplaceTempView("R_SOBERANO_WU")

#STEP 7: NORMALIZING PARTICULAR CASES (US & SOUTH KOREA)
df_rs_cleaned = sc.sql('SELECT CASE \
                            WHEN pais_bbg = "United States" THEN "United States of America" \
                            WHEN pais_bbg = "South Korea" THEN "Korea, Republic of" \
                            ELSE pais_bbg END as pais_bbg , SP, FITCH, MDY \
                        FROM R_SOBERANO_WU')

df_rs_cleaned.createOrReplaceTempView("R_SOBERANO_WU")

#STEP 8: SPLITTING UP rating_soberano.csv DATA
df_rs_wu_noagg = sc.sql("SELECT pais_bbg, SP as rating, 'SP' as agencia from R_SOBERANO_WU \
                        UNION ALL \
                        SELECT pais_bbg, FITCH as rating, 'FITCH' as agencia from R_SOBERANO_WU \
                        UNION ALL \
                        SELECT pais_bbg, MDY as rating, 'MDY' as agencia from R_SOBERANO_WU")

df_rs_wu_noagg.createOrReplaceTempView("R_SOBERANO_WU_NOAGG")

#STEP 9: GETTING orden_norma THROUGH JOINING USING rating AND agencia
df_master3_noagg_on = sc.sql('SELECT M.*, HR.orden_norma, HR.rating_norma \
                              FROM R_SOBERANO_WU_NOAGG M \
                              LEFT JOIN H_RATING HR \
                                ON M.rating=HR.rating AND M.agencia=HR.agencia_homol')
df_master3_noagg_on.createOrReplaceTempView("MASTER3_NOAGG_ON")

#STEP 10: RAKING THE DATA
df_master3_noagg_on_ranked = sc.sql('SELECT *, RANK() OVER(PARTITION BY pais_bbg ORDER BY orden_norma DESC) as rank \
                                    FROM MASTER3_NOAGG_ON')

df_master3_noagg_on_ranked.createOrReplaceTempView("MASTER3_NOAGG_ON_RANKED")

#STEP 11: GETTING rating_soberano THROUGH rank=1 AND DROPPING DUPLICATES
df_master3 = sc.sql('SELECT pais_bbg, rating_norma as rating_soberano \
                    FROM MASTER3_NOAGG_ON_RANKED \
                    WHERE rank = 1').dropDuplicates()

df_master3.createOrReplaceTempView("RATING_SOBERANO_HOMOL")

#STEP 12: INTEGRATING rating_soberano INTO THE MASTER
df_master_pro = sc.sql('SELECT rut, dv, nombre, pais, rating_empresa, RS.rating_soberano \
                        FROM MASTER2 M2 \
                        LEFT JOIN RATING_SOBERANO_HOMOL RS \
                            ON upper(M2.pais_bbg) == upper(RS.pais_bbg)')

#STEP 13: SAVING master_rating.csv
df_master_pro.toPandas().to_csv('./master_rating.csv', sep='|')

sc.stop()

print('Done!')