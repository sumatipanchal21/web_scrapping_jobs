import csv
import mysql.connector

data  = []
c = mysql.connector.connect(host='localhost', user='root', database='dice', password='12345',
                            auth_plugin='mysql_native_password')
c_obj = c.cursor()
with open("dice_jobs.csv", 'r', encoding="latin-1") as f:
    r = csv.reader(f)
    for row in r:
        data.append(row)

data_csv = "insert into dice2(Job_Title,Company_Name,description,Posted_Date,Job_Type,Location) values(%s,%s,%s,%s,%s,%s)"
c_obj.executemany(data_csv,data)
c.commit()
c_obj.close()
