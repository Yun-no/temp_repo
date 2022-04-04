import xml.etree.ElementTree as ET
import re
import mysql.connector
import requests
from requests.auth import HTTPBasicAuth

'''
update eurlex_coj.case_coj_doc_citation set cited_doc_id = (select min(id) from eurlex_coj.case_coj_doc where ecli_nr = CONCAT('ECLI:',eurlex_coj.case_coj_doc_citation.CITED_CASE_DOCNR)) where cited_doc_id is null;
update eurlex_coj.case_coj_doc_citation set cited_case_grp_id = (select case_grp_id from eurlex_coj.case_coj_doc where eurlex_coj.case_coj_doc.id = eurlex_coj.case_coj_doc_citation.cited_doc_id) where cited_case_grp_id is null and cited_doc_id is not null;
'''


if __name__ == '__main__':
    mydb = mysql.connector.connect(user='root', password='',
                                   host='127.0.0.1',
                                   database='eurlex_legalacts')
    mycursor = mydb.cursor(dictionary=True)

    mycursor.execute("""SELECT * FROM case_coj_doc_citation where cited_doc_id  in (select distinct doc_id 
    from case_coj_doc_block where block_number is not null) and id not in (select citation_id from 
    case_coj_citation_paragraph) and cited_paragraph>0""")
    myresult = mycursor.fetchall()
    for citation in myresult:
        citation_id = citation['id']
        doc_id = citation['cited_doc_id']
        paragraph_text = citation['cited_paragraph']
        paragraphlist = paragraph_text.split(',')
        for paragraph in paragraphlist:
            sql = """select id from case_coj_doc_block where doc_id = %s and (block_number =%s or block_number =%s)"""
            val = (doc_id, paragraph, paragraph+'.')
            mycursor.execute(sql, val)
            block_id_match = mycursor.fetchall()
            if len(block_id_match) > 1:
                block_id = block_id_match[-1]['id']
                insert_sql = """INSERT INTO case_coj_citation_paragraph (citation_id, cited_block_id,cited_paragraph) 
                VALUES (%s, %s, %s) """
                insert_val = (citation_id, block_id, paragraph)
                mycursor.execute(insert_sql, insert_val)
                print("Multiple match! Insert record docid: %d - paragraph: %s"%(doc_id, paragraph))
            elif len(block_id_match) == 1:
                block_id = block_id_match[0]['id']
                insert_sql = """INSERT INTO case_coj_citation_paragraph (citation_id, cited_block_id,cited_paragraph) 
                VALUES (%s, %s, %s) """
                insert_val = (citation_id, block_id, paragraph)
                mycursor.execute(insert_sql, insert_val)
                print("Insert record docid: %d - paragraph: %s"%(doc_id, paragraph))
        mydb.commit()