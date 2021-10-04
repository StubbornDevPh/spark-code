import pyspark
from pyspark import StorageLevel
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.functions import lit
import pandas as pd
import getopt
import sys
import time
import json
from time import perf_counter
import datetime
from pyspark import SparkContext,SparkConf
from pyspark.sql import SQLContext

t1_start = perf_counter()

argumentList = sys.argv[1:]
options = "i:j:o:h"
long_options = ["input1 =", "input2 =", "output =", "help"]

inputfile1 = ""
inputfile2 = ""
outputfile = ""
isH = False
try:
    # Parsing argument
    arguments, values = getopt.getopt(argumentList, options, long_options)
    # checking each argument
    for currentArgument, currentValue in arguments:
        if currentArgument in ("-h", "--help"):
            isH = True
            print("python diff.py -i <file1> -j <file2> -o <outputfile>")
        if currentArgument in ("-i", "--input1"):
            inputfile1 = currentValue
        elif currentArgument in ("-j", "--input2"):
            inputfile2 = currentValue
        elif currentArgument in ("-o", "--output"):
            outputfile = currentValue
except getopt.error as err:
    # output error, and return with an error code
    print(str(err))


if isH == False:
    conf = pyspark.SparkConf().set("spark.yarn.queue","ADS_ELT")
    sc = SparkContext(conf=conf)
    spark = SQLContext(sc)
    # spark = SparkSession.builder.master("yarn").appName("SparkAnalyzeBigData").getOrCreate()
    
    # read csv
    df1 = spark.read.format("csv").option("header", "true").load(inputfile1)
    df2 = spark.read.format("csv").option("header", "true").load(inputfile2)

    # replace blank with null
    df1 = df1.fillna(value="NULL").persist(StorageLevel.DISK_ONLY)
    df2 = df2.fillna(value="NULL").persist(StorageLevel.DISK_ONLY)

    # filter and compare column by column
    df1 = df1.select(col('rowkey'), col('plcy_nb').alias('plcy_nb_x'),col('all_cov_term_prem').alias('all_cov_term_prem_x'),col('num_coverages_policy').alias('num_coverages_policy_x'),col('pip_term_prem').alias('pip_term_prem_x'),col('apip_term_prem').alias('apip_term_prem_x'),col('comp_term_prem').alias('comp_term_prem_x'),col('ers_term_prem').alias('ers_term_prem_x'),col('bi_term_prem').alias('bi_term_prem_x'),col('coll_term_prem').alias('coll_term_prem_x'),col('pd_term_prem').alias('pd_term_prem_x'),col('rr_term_prem').alias('rr_term_prem_x'),col('mbi_term_prem').alias('mbi_term_prem_x'),col('mp_term_prem').alias('mp_term_prem_x'),col('ers_limit').alias('ers_limit_x'),col('bi_limit').alias('bi_limit_x'),col('pd_limit').alias('pd_limit_x'),col('rr_limit').alias('rr_limit_x'),col('mp_limit').alias('mp_limit_x'),col('apip_limit').alias('apip_limit_x'),col('pip_limit').alias('pip_limit_x'),col('apip_ded').alias('apip_ded_x'),col('coll_ded').alias('coll_ded_x'),col('comp_ded').alias('comp_ded_x'),col('mbi_ded').alias('mbi_ded_x'),col('pip_ded').alias('pip_ded_x'))
    df2 = df2.select(col('rowkey'), col('plcy_nb').alias('plcy_nb_y'),col('all_cov_term_prem').alias('all_cov_term_prem_y'),col('num_coverages_policy').alias('num_coverages_policy_y'),col('pip_term_prem').alias('pip_term_prem_y'),col('apip_term_prem').alias('apip_term_prem_y'),col('comp_term_prem').alias('comp_term_prem_y'),col('ers_term_prem').alias('ers_term_prem_y'),col('bi_term_prem').alias('bi_term_prem_y'),col('coll_term_prem').alias('coll_term_prem_y'),col('pd_term_prem').alias('pd_term_prem_y'),col('rr_term_prem').alias('rr_term_prem_y'),col('mbi_term_prem').alias('mbi_term_prem_y'),col('mp_term_prem').alias('mp_term_prem_y'),col('ers_limit').alias('ers_limit_y'),col('bi_limit').alias('bi_limit_y'),col('pd_limit').alias('pd_limit_y'),col('rr_limit').alias('rr_limit_y'),col('mp_limit').alias('mp_limit_y'),col('apip_limit').alias('apip_limit_y'),col('pip_limit').alias('pip_limit_y'),col('apip_ded').alias('apip_ded_y'),col('coll_ded').alias('coll_ded_y'),col('comp_ded').alias('comp_ded_y'),col('mbi_ded').alias('mbi_ded_y'),col('pip_ded').alias('pip_ded_y'))

    result = df1.join(df2,['rowkey'],how='inner')
    
    plcy_nb = result.filter(col('plcy_nb_x')==col('plcy_nb_y')).withColumn('plcy_nb_matched',lit(1)).union(result.filter(col('plcy_nb_x')!=col('plcy_nb_y')).withColumn('plcy_nb_matched',lit(0)))
    all_cov_term_prem = result.filter(col('all_cov_term_prem_x')==col('all_cov_term_prem_y')).withColumn('all_cov_term_prem_matched',lit(1)).union(result.filter(col('all_cov_term_prem_x')!=col('all_cov_term_prem_y')).withColumn('all_cov_term_prem_matched',lit(0)))
    num_coverages_policy = result.filter(col('num_coverages_policy_x')==col('num_coverages_policy_y')).withColumn('num_coverages_policy_matched',lit(1)).union(result.filter(col('num_coverages_policy_x')!=col('num_coverages_policy_y')).withColumn('num_coverages_policy_matched',lit(0)))
    pip_term_prem = result.filter(col('pip_term_prem_x')==col('pip_term_prem_y')).withColumn('pip_term_prem_matched',lit(1)).union(result.filter(col('pip_term_prem_x')!=col('pip_term_prem_y')).withColumn('pip_term_prem_matched',lit(0)))
    apip_term_prem = result.filter(col('apip_term_prem_x')==col('apip_term_prem_y')).withColumn('apip_term_prem_matched',lit(1)).union(result.filter(col('apip_term_prem_x')!=col('apip_term_prem_y')).withColumn('apip_term_prem_matched',lit(0)))
    comp_term_prem = result.filter(col('comp_term_prem_x')==col('comp_term_prem_y')).withColumn('comp_term_prem_matched',lit(1)).union(result.filter(col('comp_term_prem_x')!=col('comp_term_prem_y')).withColumn('comp_term_prem_matched',lit(0)))
    ers_term_prem = result.filter(col('ers_term_prem_x')==col('ers_term_prem_y')).withColumn('ers_term_prem_matched',lit(1)).union(result.filter(col('ers_term_prem_x')!=col('ers_term_prem_y')).withColumn('ers_term_prem_matched',lit(0)))
    bi_term_prem = result.filter(col('bi_term_prem_x')==col('bi_term_prem_y')).withColumn('bi_term_prem_matched',lit(1)).union(result.filter(col('bi_term_prem_x')!=col('bi_term_prem_y')).withColumn('bi_term_prem_matched',lit(0)))
    coll_term_prem = result.filter(col('coll_term_prem_x')==col('coll_term_prem_y')).withColumn('coll_term_prem_matched',lit(1)).union(result.filter(col('coll_term_prem_x')!=col('coll_term_prem_y')).withColumn('coll_term_prem_matched',lit(0)))
    pd_term_prem = result.filter(col('pd_term_prem_x')==col('pd_term_prem_y')).withColumn('pd_term_prem_matched',lit(1)).union(result.filter(col('pd_term_prem_x')!=col('pd_term_prem_y')).withColumn('pd_term_prem_matched',lit(0)))
    rr_term_prem = result.filter(col('rr_term_prem_x')==col('rr_term_prem_y')).withColumn('rr_term_prem_matched',lit(1)).union(result.filter(col('rr_term_prem_x')!=col('rr_term_prem_y')).withColumn('rr_term_prem_matched',lit(0)))
    mbi_term_prem = result.filter(col('mbi_term_prem_x')==col('mbi_term_prem_y')).withColumn('mbi_term_prem_matched',lit(1)).union(result.filter(col('mbi_term_prem_x')!=col('mbi_term_prem_y')).withColumn('mbi_term_prem_matched',lit(0)))
    mp_term_prem = result.filter(col('mp_term_prem_x')==col('mp_term_prem_y')).withColumn('mp_term_prem_matched',lit(1)).union(result.filter(col('mp_term_prem_x')!=col('mp_term_prem_y')).withColumn('mp_term_prem_matched',lit(0)))
    ers_limit = result.filter(col('ers_limit_x')==col('ers_limit_y')).withColumn('ers_limit_matched',lit(1)).union(result.filter(col('ers_limit_x')!=col('ers_limit_y')).withColumn('ers_limit_matched',lit(0)))
    bi_limit = result.filter(col('bi_limit_x')==col('bi_limit_y')).withColumn('bi_limit_matched',lit(1)).union(result.filter(col('bi_limit_x')!=col('bi_limit_y')).withColumn('bi_limit_matched',lit(0)))
    pd_limit = result.filter(col('pd_limit_x')==col('pd_limit_y')).withColumn('pd_limit_matched',lit(1)).union(result.filter(col('pd_limit_x')!=col('pd_limit_y')).withColumn('pd_limit_matched',lit(0)))
    rr_limit = result.filter(col('rr_limit_x')==col('rr_limit_y')).withColumn('rr_limit_matched',lit(1)).union(result.filter(col('rr_limit_x')!=col('rr_limit_y')).withColumn('rr_limit_matched',lit(0)))
    mp_limit = result.filter(col('mp_limit_x')==col('mp_limit_y')).withColumn('mp_limit_matched',lit(1)).union(result.filter(col('mp_limit_x')!=col('mp_limit_y')).withColumn('mp_limit_matched',lit(0)))
    apip_limit = result.filter(col('apip_limit_x')==col('apip_limit_y')).withColumn('apip_limit_matched',lit(1)).union(result.filter(col('apip_limit_x')!=col('apip_limit_y')).withColumn('apip_limit_matched',lit(0)))
    pip_limit = result.filter(col('pip_limit_x')==col('pip_limit_y')).withColumn('pip_limit_matched',lit(1)).union(result.filter(col('pip_limit_x')!=col('pip_limit_y')).withColumn('pip_limit_matched',lit(0)))
    apip_ded = result.filter(col('apip_ded_x')==col('apip_ded_y')).withColumn('apip_ded_matched',lit(1)).union(result.filter(col('apip_ded_x')!=col('apip_ded_y')).withColumn('apip_ded_matched',lit(0)))
    coll_ded = result.filter(col('coll_ded_x')==col('coll_ded_y')).withColumn('coll_ded_matched',lit(1)).union(result.filter(col('coll_ded_x')!=col('coll_ded_y')).withColumn('coll_ded_matched',lit(0)))
    comp_ded = result.filter(col('comp_ded_x')==col('comp_ded_y')).withColumn('comp_ded_matched',lit(1)).union(result.filter(col('comp_ded_x')!=col('comp_ded_y')).withColumn('comp_ded_matched',lit(0)))
    mbi_ded = result.filter(col('mbi_ded_x')==col('mbi_ded_y')).withColumn('mbi_ded_matched',lit(1)).union(result.filter(col('mbi_ded_x')!=col('mbi_ded_y')).withColumn('mbi_ded_matched',lit(0)))
    pip_ded = result.filter(col('pip_ded_x')==col('pip_ded_y')).withColumn('pip_ded_matched',lit(1)).union(result.filter(col('pip_ded_x')!=col('pip_ded_y')).withColumn('pip_ded_matched',lit(0)))
    
    plcy_nb1 = plcy_nb.select(col('rowkey'),col('plcy_nb_x'),col('plcy_nb_y'),col('plcy_nb_matched'))
    all_cov_term_prem1 = all_cov_term_prem.select(col('rowkey'),col('all_cov_term_prem_x'),col('all_cov_term_prem_y'),col('all_cov_term_prem_matched'))
    num_coverages_policy1 = num_coverages_policy.select(col('rowkey'),col('num_coverages_policy_x'),col('num_coverages_policy_y'),col('num_coverages_policy_matched'))
    pip_term_prem1 = pip_term_prem.select(col('rowkey'),col('pip_term_prem_x'),col('pip_term_prem_y'),col('pip_term_prem_matched'))
    apip_term_prem1 = apip_term_prem.select(col('rowkey'),col('apip_term_prem_x'),col('apip_term_prem_y'),col('apip_term_prem_matched'))
    comp_term_prem1 = comp_term_prem.select(col('rowkey'),col('comp_term_prem_x'),col('comp_term_prem_y'),col('comp_term_prem_matched'))
    ers_term_prem1 = ers_term_prem.select(col('rowkey'),col('ers_term_prem_x'),col('ers_term_prem_y'),col('ers_term_prem_matched'))
    bi_term_prem1 = bi_term_prem.select(col('rowkey'),col('bi_term_prem_x'),col('bi_term_prem_y'),col('bi_term_prem_matched'))
    coll_term_prem1 = coll_term_prem.select(col('rowkey'),col('coll_term_prem_x'),col('coll_term_prem_y'),col('coll_term_prem_matched'))
    pd_term_prem1 = pd_term_prem.select(col('rowkey'),col('pd_term_prem_x'),col('pd_term_prem_y'),col('pd_term_prem_matched'))
    rr_term_prem1 = rr_term_prem.select(col('rowkey'),col('rr_term_prem_x'),col('rr_term_prem_y'),col('rr_term_prem_matched'))
    mbi_term_prem1 = mbi_term_prem.select(col('rowkey'),col('mbi_term_prem_x'),col('mbi_term_prem_y'),col('mbi_term_prem_matched'))
    mp_term_prem1 = mp_term_prem.select(col('rowkey'),col('mp_term_prem_x'),col('mp_term_prem_y'),col('mp_term_prem_matched'))
    ers_limit1 = ers_limit.select(col('rowkey'),col('ers_limit_x'),col('ers_limit_y'),col('ers_limit_matched'))
    bi_limit1 = bi_limit.select(col('rowkey'),col('bi_limit_x'),col('bi_limit_y'),col('bi_limit_matched'))
    pd_limit1 = pd_limit.select(col('rowkey'),col('pd_limit_x'),col('pd_limit_y'),col('pd_limit_matched'))
    rr_limit1 = rr_limit.select(col('rowkey'),col('rr_limit_x'),col('rr_limit_y'),col('rr_limit_matched'))
    mp_limit1 = mp_limit.select(col('rowkey'),col('mp_limit_x'),col('mp_limit_y'),col('mp_limit_matched'))
    apip_limit1 = apip_limit.select(col('rowkey'),col('apip_limit_x'),col('apip_limit_y'),col('apip_limit_matched'))
    pip_limit1 = pip_limit.select(col('rowkey'),col('pip_limit_x'),col('pip_limit_y'),col('pip_limit_matched'))
    apip_ded1 = apip_ded.select(col('rowkey'),col('apip_ded_x'),col('apip_ded_y'),col('apip_ded_matched'))
    coll_ded1 = coll_ded.select(col('rowkey'),col('coll_ded_x'),col('coll_ded_y'),col('coll_ded_matched'))
    comp_ded1 = comp_ded.select(col('rowkey'),col('comp_ded_x'),col('comp_ded_y'),col('comp_ded_matched'))
    mbi_ded1 = mbi_ded.select(col('rowkey'),col('mbi_ded_x'),col('mbi_ded_y'),col('mbi_ded_matched'))
    pip_ded1 = pip_ded.select(col('rowkey'),col('pip_ded_x'),col('pip_ded_y'),col('pip_ded_matched'))
    final_df = plcy_nb1.join(all_cov_term_prem1,['rowkey'],how='inner')\
        .join(num_coverages_policy1,['rowkey'],how='inner')\
        .join(pip_term_prem1,['rowkey'],how='inner')\
        .join(apip_term_prem1,['rowkey'],how='inner')\
        .join(comp_term_prem1,['rowkey'],how='inner')\
        .join(ers_term_prem1,['rowkey'],how='inner')\
        .join(bi_term_prem1,['rowkey'],how='inner')\
        .join(coll_term_prem1,['rowkey'],how='inner')\
        .join(pd_term_prem1,['rowkey'],how='inner')\
        .join(rr_term_prem1,['rowkey'],how='inner')\
        .join(mbi_term_prem1,['rowkey'],how='inner')\
        .join(mp_term_prem1,['rowkey'],how='inner')\
        .join(ers_limit1,['rowkey'],how='inner')\
        .join(bi_limit1,['rowkey'],how='inner')\
        .join(pd_limit1,['rowkey'],how='inner')\
        .join(rr_limit1,['rowkey'],how='inner')\
        .join(mp_limit1,['rowkey'],how='inner')\
        .join(apip_limit1,['rowkey'],how='inner')\
        .join(pip_limit1,['rowkey'],how='inner')\
        .join(apip_ded1,['rowkey'],how='inner')\
        .join(coll_ded1,['rowkey'],how='inner')\
        .join(comp_ded1,['rowkey'],how='inner')\
        .join(mbi_ded1,['rowkey'],how='inner')\
        .join(pip_ded1,['rowkey'],how='inner')\
    # write data
    # final_df = final_df.select(col('rowkey'),col('plcy_nb_x'),col('plcy_nb_y'),col('plcy_nb_matched'),col('all_cov_term_prem_x'),col('all_cov_term_prem_y'),col('all_cov_term_prem_matched'),col('num_coverages_policy_x'),col('num_coverages_policy_y'),col('num_coverages_policy_matched'),col('pip_term_prem_x'),col('pip_term_prem_y'),col('pip_term_prem_matched'),col('apip_term_prem_x'),col('apip_term_prem_y'),col('apip_term_prem_matched'),col('comp_term_prem_x'),col('comp_term_prem_y'),col('comp_term_prem_matched'),col('ers_term_prem_x'),col('ers_term_prem_y'),col('ers_term_prem_matched'),col('bi_term_prem_x'),col('bi_term_prem_y'),col('bi_term_prem_matched'),col('coll_term_prem_x'),col('coll_term_prem_y'),col('coll_term_prem_matched'),col('pd_term_prem_x'),col('pd_term_prem_y'),col('pd_term_prem_matched'),col('rr_term_prem_x'),col('rr_term_prem_y'),col('rr_term_prem_matched'),col('mbi_term_prem_x'),col('mbi_term_prem_y'),col('mbi_term_prem_matched'),col('mp_term_prem_x'),col('mp_term_prem_y'),col('mp_term_prem_matched'),col('ers_limit_x'),col('ers_limit_y'),col('ers_limit_matched'),col('bi_limit_x'),col('bi_limit_y'),col('bi_limit_matched'),col('pd_limit_x'),col('pd_limit_y'),col('pd_limit_matched'),col('rr_limit_x'),col('rr_limit_y'),col('rr_limit_matched'),col('mp_limit_x'),col('mp_limit_y'),col('mp_limit_matched'),col('apip_limit_x'),col('apip_limit_y'),col('apip_limit_matched'),col('pip_limit_x'),col('pip_limit_y'),col('pip_limit_matched'),col('apip_ded_x'),col('apip_ded_y'),col('apip_ded_matched'),col('coll_ded_x'),col('coll_ded_y'),col('coll_ded_matched'),col('comp_ded_x'),col('comp_ded_y'),col('comp_ded_matched'),col('mbi_ded_x'),col('mbi_ded_y'),col('mbi_ded_matched'),col('pip_ded_x'),col('pip_ded_y'),col('pip_ded_matched'))
    final_df.repartition(1000).write.option(
        "header", "true").mode("overwrite").csv(outputfile)
    t1_stop = perf_counter()
    print("Elapsed time in seconds:", t1_stop-t1_start)
    ct = datetime.datetime.now()
    with open(str(ct).replace(" ", "").replace(":", ".")+".txt", "w") as log:
        log.write(str(ct)+"\n")
        log.write(f"filenames:{inputfile1},{inputfile2}\n")
        log.write(f'time:{str(t1_stop-t1_start)}')
