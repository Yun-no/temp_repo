import mysql.connector


if __name__ == '__main__':
    mydb = mysql.connector.connect(user='root', password='',
                                   host='127.0.0.1',
                                   database='echr')
    mycursor = mydb.cursor()
    mycursor = mydb.cursor(dictionary=True)
    for i in range(77):
        limit = 1000
        mycursor.execute("SELECT * FROM echr.case_coj_doc_block where block_html like '%%sC8420256%%' and block_text "
                         "like '%%OPINION%%' and opinion_title is null LIMIT %d" % limit)
        myresult = mycursor.fetchall()
        if len(myresult) == 0:
            break
        for block in myresult:
            block_id = block['id']
            block_text = block['block_text']
            block_text = block_text.decode("utf-8")
            block_text = block_text.upper()
            label = None
            a = block_text.find('OPINION OF')
            if a > 0:
                label = block_text[:a+7]
            else:
                a = block_text.find('OPINIONOF')
                if a > 0:
                    label = block_text[:a + 7]
            print(block_text)
            print(label)
            udpate_sql = """UPDATE case_coj_doc_block set opinion_title=%s, opinion_label=%s where id=%s"""
            udpate_val = (block_text,label, block_id)
            mycursor.execute(udpate_sql, udpate_val)
        mydb.commit()