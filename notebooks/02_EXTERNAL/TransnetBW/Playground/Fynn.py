# Databricks notebook source
# MAGIC %md ##Mount point on azure blob storage

# COMMAND ----------

# MAGIC %md LINKS: <br>
# MAGIC https://portal.azure.com/#blade/Microsoft_Azure_Storage/ContainerMenuBlade/overview/storageAccountId/%2Fsubscriptions%2Fa452d8c4-1385-4697-a993-e4bb0e5bba44%2FresourceGroups%2FAdvancedAnalytics%2Fproviders%2FMicrosoft.Storage%2FstorageAccounts%2Fadvancedanalyticsblob1/path/transnetbwcashforecasting/etag/%220x8D63EB07373F02E%22
# MAGIC <br> <br>
# MAGIC https://azure.microsoft.com/de-de/features/storage-explorer/

# COMMAND ----------

# MAGIC %md Material: <br>
# MAGIC Vielleicht eine Lösung: https://stackoverflow.com/questions/34028511/skipping-unknown-number-of-lines-to-read-the-header-python-pandas
# MAGIC <br>
# MAGIC def skip_to(fle, line,**kwargs):
# MAGIC     if os.stat(fle).st_size == 0:
# MAGIC         raise ValueError("File is empty")
# MAGIC     with open(fle) as f:
# MAGIC         pos = 0
# MAGIC         cur_line = f.readline()
# MAGIC         while not cur_line.startswith(line):
# MAGIC             pos = f.tell()
# MAGIC             cur_line = f.readline()
# MAGIC         f.seek(pos)
# MAGIC         return pd.read_csv(f, **kwargs)

# COMMAND ----------

spark.conf.set(
  "fs.azure.account.key.advancedanalyticsblob1.blob.core.windows.net",
  "7d734iThOdYHNF2573id5Baw0sqkResnyG4ldif0OWhs3k37wsSVGCIx4R7Fl2qkRDmrqFnX+gRUjqjqOgKnuw==")
dbutils.fs.mount(
  source = "wasbs://transnetbwcashforecasting@advancedanalyticsblob1.blob.core.windows.net",
  mount_point = "/mnt/data",
    extra_configs = {"fs.azure.account.key.advancedanalyticsblob1.blob.core.windows.net":"7d734iThOdYHNF2573id5Baw0sqkResnyG4ldif0OWhs3k37wsSVGCIx4R7Fl2qkRDmrqFnX+gRUjqjqOgKnuw=="}
)

# COMMAND ----------

dbutils.fs.unmount("/mnt/data")


Test Test Test

TESTGIT

Add this

# COMMAND ----------

# MAGIC %md ##Blob Storage files

# COMMAND ----------

# MAGIC %run /TransnetBW/Function_Definitions

# COMMAND ----------

# Extract
name = "VBRP"
separator = "\t"
content = skip_to('/dbfs/mnt/data/01_Input/'+name+'.txt', 'MANDT', sep=separator, encoding = "ISO-8859-1", skipinitialspace = True)
content = content.infer_objects()
content = content.astype(str)
content.to_parquet('/dbfs/mnt/data/100_Playground/'+name+'.parquet')

# COMMAND ----------

df = pd.read_parquet('/dbfs/mnt/data/100_Playground/'+name+'.parquet')

# COMMAND ----------

import pandas as pd

# COMMAND ----------

df = pd.read_parquet("/dbfs/mnt/data/03_DFs/BSEG.parquet")

# COMMAND ----------

def convert_func(df):
    for col in df.columns:
      if col in conv_dict:
        #df[col] = df[col].str.replace('[^\d,]', '').str.replace(',', '.', regex=False).astype(float)
        df[col] = df[col].apply(lambda x: float(x.split()[0].replace('.', '').replace(',','.')))
    return df
  #FLQITEMBS['WRBTR'] = FLQITEMBS['WRBTR'].apply(lambda x: float(x.split()[0].replace('.', '').replace(',','.')))


# COMMAND ----------

conv_dict = ["DMBTR"]

# COMMAND ----------

df = convert_func(df)

# COMMAND ----------

#'df_sap_cepc.DATAB = pd.to_datetime(df_sap_cepc.DATAB, format='%Y%m%d')
#'df_sap_cepc.DATBI = pd.to_datetime(df_sap_cepc.DATBI, format='%Y%m%d', errors = 'coerce')
#'df_sap_cepc.DATBI =  pd.to_datetime(df_sap_cepc.DATBI.fillna(datetime.datetime.strftime(pd.Timestamp.max,'%Y%m%d')))
#'df_sap_cepc

# COMMAND ----------



# COMMAND ----------

df["AUGDT"] = pd.to_datetime(df["AUGDT"], format='%d.%m.%Y', errors = 'coerce')

# COMMAND ----------

df["DMBTR"] = df["DMBTR"].astype(float)

# COMMAND ----------

df["AUGDT"]

# COMMAND ----------



# COMMAND ----------

import numpy as np

def strip_obj(col):
    if col.dtypes == object:
        return (col.astype(str)
                   .str.strip()
                   .replace({'nan': np.nan}))
    return col

df = df.apply(strip_obj, axis=0)

# COMMAND ----------

df.head()

# COMMAND ----------

df.dtypes

# COMMAND ----------

df = df.apply(lambda x: x.str.strip() if x.dtype == "object" else x)

# COMMAND ----------

# Transformieren
#df = pd.read_parquet('/dbfs/mnt/data/100_Playground/'+name+'.parquet')
#df.replace({'nan' : np.nan ,'00:00:00' : np.nan , "0,00" : "0"  , "0" : np.nan}, inplace = True)
#df.replace({"0,00" : "0"}, inplace = True)
#df["KZWI5"] = df["KZWI5"].astype(str)
#df["KZWI5"] = df["KZWI5"].astype(float).astype(int)
df.dropna(axis='columns', how='all', inplace = True)
#df = convert_func(df)
#df = type_transform(df)

# COMMAND ----------

df["KZWI6"]

# COMMAND ----------

df.head()

# COMMAND ----------

df.to_csv('/dbfs/mnt/data/100_Playground/'+name+'.csv')

# COMMAND ----------

df_parquet = pd.read_parquet('/dbfs/mnt/data/MARA.parquet', engine='pyarrow')

# COMMAND ----------

#print(df_parquet.columns)
type_transform(df_parquet)

# COMMAND ----------

def skip_to(fle, line,**kwargs):
    if os.stat(fle).st_size == 0:
        raise ValueError("File is empty")
    with open(fle,encoding = "ISO-8859-1") as f:
        pos = 0
        cur_line = f.readline()
        print(cur_line)
        print(cur_line.find(line))
        while not cur_line.find(line) != -1:
            pos = f.tell()
            print(pos)
            cur_line = f.readline()
        print(pos)
        f.seek(pos)
        return pd.read_csv(f, **kwargs)

# COMMAND ----------

def skip_to(fle, line,**kwargs):
    if os.stat(fle).st_size == 0:
        raise ValueError("File is empty")
    with open(fle,encoding = "ISO-8859-1") as f:
        pos = 0
        cur_line = f.readline()
        print(cur_line)
        #print(cur_line.find(line))
        while cur_line.find(line) == -1:
            pos = f.tell()
            #print(pos)
            cur_line = f.readline()
        #print(pos)
        f.seek(pos)
        return pd.read_csv(f, **kwargs)

# COMMAND ----------

df_testVBAK = skip_to('/dbfs/mnt/data/VBAK.txt', 'MANDT', sep="\t", encoding = "ISO-8859-1", skipinitialspace=True)

# COMMAND ----------

df_testVBAK

# COMMAND ----------

df_testVBAK.dropna(axis='columns', how='all', inplace=True)

# COMMAND ----------

#df_testVBAK.columns = df_testVBAK.columns.str.strip()
#df_testVBAK["NETWR"] = pd.core.strings.str_strip(df_testVBAK['NETWR'])
df_testVBAK["NETWR"] = df_testVBAK["NETWR"].str.strip()

# COMMAND ----------

print(float(str(df_testVBAK["NETWR"][0]).replace(".","").replace(",",".")))

# COMMAND ----------

df_testVBAK ["NETWR"] = df_testVBAK['NETWR'].astype("float")

# COMMAND ----------

#df_testVBAK['ERDAT'] = pd.to_datetime(df_testVBAK['ERDAT'])
#df_testVBAK['ANGDT'] = pd.to_datetime(df_testVBAK['ANGDT'])
#df_testVBAK['ERDAT'] = df_testVBAK['ERDAT'].dt.strftime('%d.%m.%Y')
#df_testVBAK['ERDAT'] = df_testVBAK['ERDAT'].astype("datetime64[ms]")
df_testVBAK['NETWR'] = df_testVBAK['NETWR'].astype("decimal")

# COMMAND ----------

df_testVBAK.head()
df_testVBAK.dtypes

# COMMAND ----------

df_BKPF = skip_to('/dbfs/mnt/data/BKPF.txt', 'MANDT', sep="|", encoding = "ISO-8859-1")
df_BSAD = skip_to('/dbfs/mnt/data/BKPF.txt', 'MANDT', sep="\t", encoding = "ISO-8859-1")
df_EKKO = skip_to('/dbfs/mnt/data/EKKO.txt', 'MANDT', sep="\t", encoding = "ISO-8859-1")
df_EKPO = skip_to('/dbfs/mnt/data/EKPO.txt', 'MANDT', sep="\t", encoding = "ISO-8859-1")
df_FLQITEMBS = skip_to('/dbfs/mnt/data/FLQITEMBS.txt', 'MANDT', sep="\t", encoding = "ISO-8859-1")
df_KNA1 = skip_to('/dbfs/mnt/data/KNA1.txt', 'MANDT', sep="\t", encoding = "ISO-8859-1")
df_LFA1 = skip_to('/dbfs/mnt/data/LFA1.txt', 'MANDT', sep="\t", encoding = "ISO-8859-1")
df_MARA = skip_to('/dbfs/mnt/data/MARA.txt', 'MANDT', sep="\t", encoding = "ISO-8859-1")
df_RBKP = skip_to('/dbfs/mnt/data/RBKP.txt', 'MANDT', sep="\t", encoding = "ISO-8859-1")
df_RSEG = skip_to('/dbfs/mnt/data/RSEG.txt', 'MANDT', sep="\t", encoding = "ISO-8859-1")
df_VBAK = skip_to('/dbfs/mnt/data/VBAK.txt', 'MANDT', sep="\t", encoding = "ISO-8859-1")
df_VBAP = skip_to('/dbfs/mnt/data/VBAP.txt', 'MANDT', sep="\t", encoding = "ISO-8859-1")
df_VBFA = skip_to('/dbfs/mnt/data/VBFA.txt', 'MANDT', sep="\t", encoding = "ISO-8859-1")
df_VBPA = skip_to('/dbfs/mnt/data/VBPA.txt', 'MANDT', sep="\t", encoding = "ISO-8859-1")
df_VBRK = skip_to('/dbfs/mnt/data/VBRK.txt', 'MANDT', sep="\t", encoding = "ISO-8859-1")
df_VBRP = skip_to('/dbfs/mnt/data/VBRP.txt', 'MANDT', sep="\t", encoding = "ISO-8859-1")

# COMMAND ----------

df_LFA1 = skip_to('/dbfs/mnt/data/LFA1.txt', 'MANDT', sep="\t", encoding = "ISO-8859-1")

# COMMAND ----------

df_KNA1.head()

# COMMAND ----------

#dieser loop gibt eine liste aller enthaltenen .txt files
stack = list(dbutils.fs.ls("/mnt/data"))
file_list = list()
while stack:
  cur_dir = stack.pop()
  files = list(dbutils.fs.ls(cur_dir.path))
  txt_files = list(filter(lambda file: file.name.endswith(".txt"),files))
  directories = [file for file in files if file.size == 0]
  print(directories)
  if txt_files:
    file_list.extend(txt_files)
  stack.extend(directories) 
print(file_list)

# COMMAND ----------

#generate list of text file paths in /mnt/data
stack = list(dbutils.fs.ls("/mnt/data"))
mydocuments = [x.path for x in stack if x.path.endswith('.txt')]
print(mydocuments)


# COMMAND ----------



# COMMAND ----------

# MAGIC %md Get separator:

# COMMAND ----------

def _get_separator(filename,encoding='CP1252'):
    import pandas as pd
    #import csv
    #s = csv.Sniffer()
    #with open(filename, "r",encoding=encoding) as file:
    #  sep = s.sniff(file.read(4096*1024)).delimiter
    reader = pd.read_csv(filename, sep=None, iterator=True,engine='python',nrows=10,encoding= encoding)
    sep_str = reader._engine.data.dialect.delimiter
    reader.close()
    return sep_str 

# COMMAND ----------



# COMMAND ----------

df.dropna(axis='columns', how='all', inplace=True)
#used to drop empty columns

# COMMAND ----------

# MAGIC %md Todo: Zeilenanzahl der Files validieren --> Festhalten und gegen Screenshots vergleichen

# COMMAND ----------

# MAGIC %md Todo: Parquet file aus Df erzeugen, vorher Datentyp der Columns über Dictionary festlegen (zumindest für die wichtigen Schlüssel) und in blob storage schreiben
# MAGIC (pd.read_parquet pd.to_parquet)

# COMMAND ----------

# MAGIC %md Todo: Notebook behalten aber den Prozess in einzelen Notebooks abbilden (zB 01_ReadTransformWrite, 00_Function_Definition)

# COMMAND ----------

df.shape

# COMMAND ----------

#funktioniert
file = open('/dbfs/mnt/data/VBAK.txt', encoding = "ISO-8859-1")
print (file.readlines(1000))
print (file.name)

##klären
#header_row_dict = {"VBAK":"4", "BSEG":"10"}
#for file in files:
#  if file.name in header_row_dict:
#    df = pd.read_csv(file.path, header = header_row_dict[file.name])

# COMMAND ----------

#todo: für fynn klären
#file = open('/dbfs/mnt/data/VBAK.txt', encoding = "ISO-8859-1") 
#print file.readlines()


#@fynn: damit kannst du sonst schon mal loslaufen
with open("/dbfs/mnt/data/VBAK.txt",encoding = "ISO-8859-1" ) as file:
    content = file.readlines()
  
content

# COMMAND ----------

df.to_parquet(str("/dbfs" + output_folder_single + "/" + table + ".parquet")) 
df.to_parquet(filepath)

# COMMAND ----------

df_VBAK = pd.read_csv("/dbfs/mnt/data/VBAK.txt", sep='\t', encoding = "ISO-8859-1", skipinitialspace=True, skiplines)

# COMMAND ----------

df_VBAK.head()

# COMMAND ----------

df_VBAK.dropna(axis='columns', how='all') # Löscht ALLE Spalten die VOLLSTÄNDIG "leer" sind

# COMMAND ----------

df_VBAK.dtypes

# COMMAND ----------

df_VBAK.describe()

# COMMAND ----------

df_VBAK.shape

# COMMAND ----------

column_dict_ger = {"MANDT":"Mandant",
                   "WRBTR":"Transaktionswährung",
                   "VBELN":"Vertriebsbeleg",
                   "EBELN":"Einkaufsbeleg",
                   "EBELP":"Einkaufsbeleg Position",
                   "BELNR":"Buchhaltungsbeleg",
                   "GJHAR":"Geschäftsjahr",
                   "BUKRS":"Buchungskreis",
                   "MATNR":"Materialnummer",
                   "LIFNR":"Kontonummer Lieferant",
                   "POSNR":"Fakturaposition",
                   "ZBUKR":"Zahlender Buchungskreis",
                   "GSBER":"Geschäftsbereich",
                   "KUNNR":"Kunde",
                   "PERNR":"Personalnummer",
                   "PARNR":"Ansprechpartner-Nummer",
                   "ADRNR":"Adresse",
                   "SGTXT":"Positionstext",
                   "ZUONR":"Zuordnungsnummer",
                   "PSWSL":"Währung Hauptbuch",
                   "PSWBT":"Betrag Hauptbuch",
                   "NETWR":"Nettowert",
                   "WAERK":"Währung Vertriebsbeleg"
                  }

# COMMAND ----------

def type_transform(df):
  for col in df.columns:
      if col in type_dict:
        #print(col)
        df[col] = df[col].astype(type_dict.get(col))
        #print(col)
        #print(df[col])
        #print(df[col].dtype)
    #except:
      #pass
  return df

# COMMAND ----------

def col_rename(df):
  for col in df.columns:
      if col in column_dict_ger:
        #print([col])
        #x = column_dict_ger.get(col)
        #print(x)
        df.rename(columns={(col):column_dict_ger.get(col)}, inplace=True)
  return df

# COMMAND ----------

column_dict_ger.get("MANDT")

# COMMAND ----------

col_rename(test_VBAK)

# COMMAND ----------

test_VBAK = pd.read_parquet('/dbfs/mnt/data/03_DFs/VBAK.DF.parquet')

# COMMAND ----------

test_VBAK

# COMMAND ----------

test_VBAK.rename(columns=column_dict_ger, inplace=True)
#df.rename(index=str, columns={"A": "a", "B": "c"})

# COMMAND ----------

test_VBAK

# COMMAND ----------

test_VBAK.rename(columns={"Mandant":"Test"})

# COMMAND ----------

column_dict_ger