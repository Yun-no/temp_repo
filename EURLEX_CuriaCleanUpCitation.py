import mysql.connector
import xml.etree.ElementTree as ET


if __name__ == '__main__':
    mydb = mysql.connector.connect(user='root', password='',
                                   host='127.0.0.1',
                                   database='eurlex_legalacts')

    mycursor = mydb.cursor(dictionary=True)


    mycursor.execute("SELECT block_id,cited_block_id FROM eurlex_legalacts.case_coj_doc_citation a where (select count(*) "
                     "from eurlex_legalacts.case_coj_doc_citation b where b.block_id=a.block_id and "
                     "b.cited_block_id=a.cited_block_id and b.cited_block_id is not null)>1 group by block_id,"
                     "cited_block_id")
    myresult = mycursor.fetchall()

    for block in myresult:
        block_id = block['block_id']
        cited_block_id = block['cited_block_id']
        mycursor.execute(
            "SELECT * FROM eurlex_legalacts.case_coj_doc_citation a where (select count(*) "
            "from eurlex_legalacts.case_coj_doc_citation b where b.block_id=a.block_id and "
            "b.cited_block_id=a.cited_block_id and b.cited_block_id is not null)>1 and block_id=%d "
            "and cited_block_id=%d"%(block_id, cited_block_id))
        myresult1 = mycursor.fetchall()
        cited_case_text_list = []
        for i in range(len(myresult1)):
            c = myresult1[i]
            c_id = c['id']
            c_text = c['cited_case_text']
            if c_text not in cited_case_text_list:
                cited_case_text_list.append(c_text)
            if i < len(myresult1)-1:
                delete_sql = """DELETE FROM case_coj_doc_citation where id=%d"""%(c_id)
                print(delete_sql)
                mycursor.execute(delete_sql)
            else:
                new_text = " ".join(cited_case_text_list)
                udpate_sql = """UPDATE case_coj_doc_citation set cited_case_text=%s where id=%s"""
                udpate_val = (new_text, c_id)
                print(udpate_sql)
                mycursor.execute(udpate_sql, udpate_val)

        mydb.commit()