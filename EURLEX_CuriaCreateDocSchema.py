import lxml.html as HT
from lxml import etree
import re
import mysql.connector
import xml.etree.ElementTree as ET


if __name__ == '__main__':
    mydb = mysql.connector.connect(user='root', password='',
                                   host='127.0.0.1',
                                   database='eurlex_legalacts')
    mycursor = mydb.cursor(dictionary=True)


    mycursor.execute("SELECT id, ecli_nr, celex_id, webservice_result,doc_date FROM case_coj_doc")
    myresult = mycursor.fetchall()
    passby = "Court of Justice"
    for doc in myresult:
        doc_id = doc['id']
        doc_date = doc['doc_date']
        celex_id = doc['celex_id']
        if celex_id is None:
            celex_id = ''
        ecli_nr = doc['ecli_nr']
        print(celex_id)
        title = ''
        ws_xml = doc["webservice_result"]
        if ws_xml is not None:
            ws_xml = ws_xml.decode()
            root = ET.fromstring(ws_xml)
            ns = {'env': 'http://www.w3.org/2003/05/soap-envelope',
                  'S': 'http://www.w3.org/2003/05/soap-envelope',
                  'ns0': 'http://eur-lex.europa.eu/search'}
            results = root.findall('.//ns0:content', ns)
            for content in results:
                notice = content.find('ns0:NOTICE', ns)
                expression = notice.find('ns0:EXPRESSION', ns)
                try:
                    title_node = expression.find('ns0:EXPRESSION_TITLE', ns)
                except:
                    continue
                if title_node is not None:
                    title = title_node.find('ns0:VALUE', ns)
                    title = title.text
        title = title.replace('"', '')
        schema = """{
        "@context": "https://schema.org/",
        "@type": "Legislation",
        "@id": "%s",
        "legislationIdentifier": "%s",
        "name": "%s",
        "legislationType": "Case-law",
        "legislationPassedBy": "Court of Justice",
        "legislationDate": "%s"
    }""" % (ecli_nr, celex_id, title, doc_date)

        udpate_sql = """UPDATE case_coj_doc set schema_json=%s where id=%s"""
        udpate_val = (schema, doc_id)
        mycursor.execute(udpate_sql, udpate_val)

        mydb.commit()
        continue






