# Databricks notebook source
# DBTITLE 1,Import
from pyspark.sql.functions import col
from datetime import datetime

# COMMAND ----------

# DBTITLE 1,Widgets
dbutils.widgets.text("app","PMFSALE")
dbutils.widgets.text("phase","dev")

# COMMAND ----------

# DBTITLE 1,Parameterization 
app= dbutils.widgets.get("app")
phase = dbutils.widgets.get("phase")

read_options = {'inferschema':True}

df = spark.read.csv(f"/FileStore/mdip/history_load_{app}_{phase}.csv", header=True, inferSchema=True)

# COMMAND ----------

df.display()

# COMMAND ----------

# DBTITLE 1,Read the data from source and write into target.
df_one_to_one = df.filter((col("hstry_load")=="Yes") & (col("split")=="No") & (col("rm_src")=="No")).distinct()
l1 = df_one_to_one.collect()
c1 = df_one_to_one.count()
print("No of tables for File Transfer :" )
print(c1)
print("Started the transfering ...")
print("Processed tbl list :")
tbl_nm,src,trgt = [],[],[]
for i in range(c1):
    tbl_nm.append(l1[i][0])
    src.append(l1[i][4])
    trgt.append(l1[i][5])
for src,trgt,tbl_nm in zip(src,trgt,tbl_nm):
    temp_df = spark.read.load(f"{src}",**read_options)
    temp_df.write.format("delta").mode("overwrite").save(f"{trgt}")
    print (tbl_nm)

# COMMAND ----------

# DBTITLE 1,Delete the existing data from target path
df_tbl_clng = df.filter((col("hstry_load")=="Yes") & (col("split")=="Yes") & (col("rm_src")=="Yes")).select("trgt").distinct()
l2 = df_tbl_clng.collect()
c2 = df_tbl_clng.count()
print("No of tables for Delete :" )
print(c2)
print("Deleting the records from tbl .....")
trgt_path = []
for i in range(c2):
        trgt_path.append(l2[i][0])
for trgt_path in (trgt_path):
    temp_df = spark.sql(f"delete from delta.`{trgt_path}` where 1=1")
    print (trgt_path)


# COMMAND ----------

# DBTITLE 1,Merge the split files in to single file
df_for_mrg = df.filter((col("hstry_load")=="Yes") & (col("split")=="Yes")).distinct()
s = df_for_mrg.select("trgt").distinct()
l3 = df_for_mrg.collect()
c3 = df_for_mrg.count()
print("No of tbl for mrg oprtn" )
print(s.count())
print("Merging ....")
print("completed tbl list :")
tbl_nm,src,trgt = [],[],[]
for i in range(c3):
    tbl_nm.append(l3[i][0])
    src.append(l3[i][4])
    trgt.append(l3[i][5])
for src,trgt,tbl_nm in zip(src,trgt,tbl_nm):
    temp_df = spark.read.load(f"{src}",**read_options)
    temp_df.write.format("delta").mode("append").save(f"{trgt}")
    print (tbl_nm)

# COMMAND ----------

# DBTITLE 1,Count check 1 :  Merged tables 
df_mrgd_tbl_cnt_ck = df.filter((col("hstry_load")=="Yes") & (col("split")=="Yes")).select("trgt").distinct()
l4 = df_mrgd_tbl_cnt_ck.collect()
c4 = df_mrgd_tbl_cnt_ck.count()

mrgd_tbl_cnt = {}
mrgd_tbl_nm= []
for i in range(c4):
    mrgd_tbl_nm.append(l4[i][0])
#print(hstry_tbl_nm)

for index, tbl_nm in enumerate(mrgd_tbl_nm, start=1):
    query = f"SELECT COUNT(*) as count FROM delta.`{tbl_nm}`"
    result = spark.sql(query)
    count = result.collect()[0]['count']
    mrgd_tbl_cnt[index] = {tbl_nm: count}

print("Merged Table Count:")
for index, tbl_cnt in mrgd_tbl_cnt.items():
    tbl_nm, count = list(tbl_cnt.items())[0]
    print(f"{index}. {tbl_nm}: {count}")

# COMMAND ----------

# DBTITLE 1, Count check 2 : EDM tables 
df_hstry_tbl_cnt_ck = df.filter(col("hstry_load") == "Yes").select("src")
l5 = df_hstry_tbl_cnt_ck.collect()
c5 = df_hstry_tbl_cnt_ck.count()

hstry_tbl_cnt = {}
hstry_tbl_nm= []
for i in range(c5):
    hstry_tbl_nm.append(l5[i][0])

for index, tbl_nm in enumerate(hstry_tbl_nm, start=1):
    query = f"SELECT COUNT(*) as count FROM delta.`{tbl_nm}`"
    result = spark.sql(query)
    count = result.collect()[0]['count']
    hstry_tbl_cnt[index] = {tbl_nm: count}

print("History Table Count:")
for index, tbl_cnt in hstry_tbl_cnt.items():
    tbl_nm, count = list(tbl_cnt.items())[0]
    print(f"{index}. {tbl_nm}: {count}")

# COMMAND ----------

spark.sql(f" delete from delta.`abfss://silver@cgfmdipdevadls2.dfs.core.windows.net/silver-landing/Test/DW_VNDR` where 1=1")

# COMMAND ----------

spark.sql(f" delete from delta.`abfss://silver@cgfmdipdevadls2.dfs.core.windows.net/silver-landing/Test/CONC_EMPLYE_CMNTS_DTL` where 1=1")

