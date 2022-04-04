import lxml.html as HT
from lxml import etree
import re
import mysql.connector
import zlib


if __name__ == '__main__':
    mydb = mysql.connector.connect(user='root', password='',
                                   host='127.0.0.1',
                                   database='eurlex_legalacts')
    mycursor = mydb.cursor(dictionary=True)

    #batch=100
    #loop=2#130

    #for i in range(loop):
    if 1:
        count=0
        mycursor.execute("SELECT id, celex_id FROM eurlex_doc""")
        myresult = mycursor.fetchall()
        for doc in myresult:
            doc_id = doc['id']
            celex_id = doc['celex_id']
            sector = celex_id[0]
            doc_type = "".join(re.findall("[a-zA-Z]+", celex_id[:8]))
            sql = """UPDATE eurlex_doc SET doc_type = %s, sector = %s WHERE id=%s"""
            print(celex_id, sector, doc_type)
            val = (doc_type, sector, doc_id)
            mycursor.execute(sql, val)
            count = count +1
            if count > 1000:
                mydb.commit()
                count=0







