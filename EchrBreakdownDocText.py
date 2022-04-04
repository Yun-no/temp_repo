import lxml.html as HT
from lxml import etree
import re
import mysql.connector


def break_down_text(mydb, limit):
    mycursor = mydb.cursor(dictionary=True)
    mycursor.execute("SELECT id,doc_content_html FROM case_coj_doc WHERE has_html = 1 and id not in (select "
                     "distinct(doc_id) from case_coj_doc_block) LIMIT %d"%limit)

    i = 0

    myresult = mycursor.fetchall()
    for doc in myresult:
        i=i+1
        doc_id = doc['id']
        print(doc_id)
        doc_content_html = doc['doc_content_html']

        try:
            doc_content = HT.fromstring(doc_content_html)
        except:
            sql = """UPDATE case_coj_doc SET has_html = 0 WHERE id=%s"""
            val = (doc_id,)
            mycursor.execute(sql, val)
            continue
        ###check content matching old structure
        nodes = doc_content.xpath("//p")
        print(nodes)
        for p in nodes:
            try:
                p_html = etree.tostring(p, pretty_print=True)
                p_text = p.text_content()
            except:
                continue
            p_id = ""

            sql = """insert into case_coj_doc_block (doc_id,block_html,block_text,block_number)
                            values (%s,%s,%s,%s)"""
            val = (doc_id, p_html, str(p_text), str(p_id))
            print(p_id, p_html)
            mycursor.execute(sql, val)
        mydb.commit()
    return i

mydb = mysql.connector.connect(user='root', password='',
                               host='127.0.0.1',
                               database='echr')
if __name__ == '__main__':
    for i in range(644):
        doc_count = break_down_text(mydb, 100)
        if doc_count == 0:
            break






