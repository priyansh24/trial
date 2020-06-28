from pymongo import MongoClient
from datetime import datetime, timedelta
import pandas as pd
import mysql.connector

client = MongoClient('mongodb+srv://Priyansh:Priyansh@cluster0-0wsdg.mongodb.net/test?retryWrites=true&w=majority')
db = client.Records
collection = db.apiFailedRequest

end = datetime.now()
end = datetime(2020, 6, 16, 6, 0, 0)
start = end - timedelta(hours=1)

print('fetching data from mongo')
res = list(collection.find({"authkey": {"$ne": None },"dateTime": {"$gt": start , "$lt": end}}, {"authkey": 1, "reason": 1,"_id":0}))
print('Successwfully data got fetched')
df = pd.DataFrame.from_records(res)
print (df.shape)
df_agg = (df.groupby(['authkey', 'reason']).size())
print (df_agg)
#df_agg = df_agg.reset_index()
#df_agg.rename(columns={"0": "count"}, inplace=True)
#set_1 = set(df['authkey'].tolist())
#set_2 = set(df_agg['authkey'].tolist())
#print("difference ",len(set_1),len(set_2))



mydb = mysql.connector.connect(
    host="localhost",
    user="root",
    password="priyansh",
    database="Internship"
    )

mycursor = mydb.cursor()
sql = "INSERT INTO apiFailedREquest (authkey,reason,count) VALUES (%s,%s,%s)"

print ('creating list to insert bulk data')
arg_list=[]
for items in df_agg.iteritems(): 
    authkey = items[0][0]
    reason = items[0][1]
    count = items[1]
    val = (authkey, reason, count)
    arg_list.append(val)
print ('list creation done')

    
mycursor.executemany(sql, arg_list)
print('data inserted successfully ')
mydb.commit()




