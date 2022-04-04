import mysql.connector
import lxml.html as HT
import re
import pickle


global case_dict

def get_echr_citation(block_text, block_id, doc_id, app_nr, mycursor):
    try:
        if type(block_text) is bytes:
            block_text = block_text.decode()
        block_text = re.sub(' +', ' ', block_text)
        block_text = block_text.replace('\r', '')
        block_text = block_text.replace('\n', '')
        citation_pattern = re.compile(r'\d+/\d+')
        citations = re.findall(citation_pattern, block_text)
    except:
        return False
    for citation in citations:
        if citation != app_nr:
            cited_doc_id = None
            citationlast = citation.split('/')[-1]
            if len(citationlast) == 2:
                if citation not in case_dict:
                    mycursor.execute("""SELECT max(id) as id from case_coj_doc where ecli_nr is not null and application_nr ='%s'""" % citation)
                    docs = mycursor.fetchall()
                    if len(docs)>0 and docs[0]['id'] is not None:
                        cited_doc_id = docs[0]['id']
                        case_dict[citation] = cited_doc_id
                else:
                    cited_doc_id = case_dict[citation]
            print(citation, cited_doc_id)
            insert_sql = """INSERT INTO case_coj_doc_citation (block_id, doc_id, cited_doc_id, cited_case_docnr, 
            citation_text) VALUES (%s, %s, %s, %s, %s) """
            insert_val = (block_id, doc_id, cited_doc_id, citation, citation)
            mycursor.execute(insert_sql, insert_val)




def get_citation_from_block_html(mydb, limit):
    mycursor = mydb.cursor(dictionary=True)
    mycursor.execute("SELECT id,application_nr FROM case_coj_doc WHERE citation_built = 0 LIMIT %d" % limit)
    myresult = mycursor.fetchall()
    doc_count = len(myresult)
    for doc in myresult:
        doc_id = doc['id']
        app_nr = doc['application_nr']
        mycursor.execute("""SELECT * from case_coj_doc_block where doc_id=%s""" % doc_id)
        blocks = mycursor.fetchall()
        for block in blocks:
            block_id = block['id']
            block_text = block['block_text']
            get_echr_citation(block_text, block_id, doc_id, app_nr, mycursor)
        sql = "UPDATE case_coj_doc SET citation_built=%s where id = %s"
        val = (1, doc_id)
        mycursor.execute(sql, val)
        mydb.commit()
    return doc_count


def get_echr_page_num(html):
    pagenum = None
    try:
        html_content = HT.fromstring(html)
    except:
        return None
    spans = html_content.xpath('//p[@class="s30EEC3F8"]/span[@class="sB8D990E2"]')
    if len(spans) > 1:
        span0 = spans[0].text_content().strip()
        span1 = spans[1].text_content().strip()
        if span0.isdigit() and span1 == '.':
            pagenum = span0
            print(span0)
            print(html)
            return pagenum
    spans = html_content.xpath('//p[@class="s899B3E47"]/span[@class="s68F5EAEF"]')
    if len(spans) > 1:
        span0 = spans[0].text_content().strip()
        if re.match(r"\d\.", span0):
            pagenum = span0.replace('.','')
            print(pagenum)
            print(html)
            return pagenum
    return pagenum



def get_pragraph_from_block_html(mydb, limit):
    doc_count = 0
    mycursor = mydb.cursor(dictionary=True)
    mycursor.execute("SELECT id,application_nr FROM case_coj_doc WHERE citation_built = 0 order by id desc LIMIT %d" % limit)
    myresult = mycursor.fetchall()
    for doc in myresult:
        doc_id = doc['id']
        print(doc_id)
        doc_count += 1
        app_nr = doc['application_nr']
        mycursor.execute("""SELECT * from case_coj_doc_block where doc_id=%s""" % doc_id)
        blocks = mycursor.fetchall()
        has_pgnum = -1
        for block in blocks:
            block_id = block['id']
            block_html = block['block_html']

            pg_num = get_echr_page_num(block_html)

            if pg_num is not None:
                has_pgnum = 1
                udpate_sql = """UPDATE case_coj_doc_block set block_number=%s where id=%s"""
                udpate_val = (pg_num, block_id)
                mycursor.execute(udpate_sql, udpate_val)
        sql = "UPDATE case_coj_doc SET citation_built=%s where id = %s"
        val = (has_pgnum, doc_id)
        mycursor.execute(sql, val)
        mydb.commit()
    return doc_count



mydb = mysql.connector.connect(user='root', password='',
                               host='127.0.0.1',
                               database='echr')
mycursor = mydb.cursor()



if __name__ == '__main__':
    with open('saved_casedict.pkl', 'rb') as f:
        case_dict = pickle.load(f)
    for i in range(78):
        doc_count = get_citation_from_block_html(mydb, 1000)
        if doc_count == 0:
            break
