import pandas as pd
import statistics
import time
import glob

import mysql.connector
from mysql.connector.constants import ClientFlag

mydb = mysql.connector.connect(
    host=''#your remote sqlhost name,
    user=''#your login,
    passwd=''#your password,
    database=''#database name,
    allow_local_infile=True
)

cur = mydb.cursor()

#import .tsv file and create dataframe
def importClean(datasetFile):
    df = pd.read_csv(datasetFile, sep='\t', error_bad_lines=False, warn_bad_lines=False)
    df['review_date'] = pd.to_datetime(df['review_date'], errors='coerce')
    df['star_rating'] = pd.to_numeric(df['star_rating'], errors='coerce')
    df['helpful_votes'] = pd.to_numeric(df['helpful_votes'], errors='coerce')
    df['total_votes'] = pd.to_numeric(df['total_votes'], errors='coerce')

    return df

#create mysql table
def createTable(name):
    creationString = "CREATE TABLE " + name + "("
    creationString += "marketplace VARCHAR(4),"
    creationString += "customer_id VARCHAR(15),"
    creationString += "review_id VARCHAR(20),"
    creationString += "product_id VARCHAR(20),"
    creationString += "product_parent VARCHAR(20),"
    creationString += "product_title VARCHAR(110),"
    creationString += "product_category VARCHAR(23),"
    creationString += "star_rating TINYINT,"
    creationString += "helpful_votes INT,"
    creationString += "total_votes INT,"
    creationString += "vine BOOLEAN,"
    creationString += "verified_purchase BOOLEAN,"
    creationString += "review_headline VARCHAR(100),"
    creationString += "review_body VARCHAR(500),"
    creationString += "review_date DATE);"
    cur.execute(creationString)
    mydb.commit()

#add path to dataset directory before *
allFiles = glob.glob('*.tsv')
for i in allFiles:
    #add path to dataset directory
    s = i.replace('','')
    s = s.replace('.tsv','')
    print('creating table',s)
    createTable(s)
    #note: usage of LOAD DATA LOCAL INFILE may vary depending on MySQL version and database permissions
    loadDataString = 'LOAD DATA LOCAL INFILE \'' + i + '\' INTO TABLE ' + s + ';'
    print(loadDataString)
    cur.execute(loadDataString)
    mydb.commit()
    
