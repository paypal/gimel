import subprocess
import sys
import os


hbase_table=os.environ["HBASE_TABLE"]
hbase_namespace="default"
hiveDatabase = os.environ["HIVE_DB"]
if len(hbase_table.split(":"))==2:
    hbase_namespace=hbase_table.split(":")[0]
    hbase_table=hbase_table.split(":")[1]

if (hiveDatabase is ""):
    hiveDatabase = "default"


alldata=set()
count=0
for line in sys.stdin:
    count +=1
    #    sys.stdout.write("\r%d-%d" % (len(alldata),count))
    alldata.add(line.split(",")[0]),
alldata.remove('\n')

allcolumns = []
for column in alldata:
    if not column.startswith(">"):
        allcolumns.append(column)

ddl="""
CREATE EXTERNAL TABLE IF NOT EXISTS """+hiveDatabase+""".pc_hbase_"""+ hbase_namespace+"_"+hbase_table +"""
(
`"""+ hbase_table +"_key` string"+"""
""" + '\n'.join(",`"+column.split(":")[1]+"` string" for column in allcolumns) + """
)
STORED BY 'org.apache.hadoop.hive.hbase.HBaseStorageHandler'
WITH SERDEPROPERTIES ("hbase.columns.mapping" =
":key"""+ ''.join(","+column for column in allcolumns)+'\")'+"""
TBLPROPERTIES ("hbase.table.name" = """ + '"'+hbase_namespace+":"+hbase_table+'");'

print(ddl+"\n\n")