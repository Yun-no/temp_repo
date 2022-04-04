import lxml.html as HT
from lxml import etree
import re
import mysql.connector


if __name__ == '__main__':
    mydb = mysql.connector.connect(user='root', password='',
                                   host='127.0.0.1',
                                   database='eurlex_legalacts')
    mycursor = mydb.cursor(dictionary=True)


    mycursor.execute("SELECT id, celex_id, doc_content_html FROM case_coj_doc WHERE id not in (select doc_id from "
                     "case_coj_doc_block) and doc_content_html is not null order by id desc")
    myresult = mycursor.fetchall()
    for doc in myresult:
        doc_id = doc['id']
        celex_id = doc['celex_id']
        print(celex_id)
        doc_content_html = doc['doc_content_html']
        doc_content = HT.fromstring(doc_content_html)
        ###check content matching old structure
        doc_text = doc_content.cssselect("div#TexteOnly")
        if len(doc_text) > 0:
            doc_text = doc_text[0]
            titles = doc_text.xpath("./h2")
            for title in titles:
                title_html = etree.tostring(title, pretty_print=True)
                title_text = title.text_content()
                sql = """insert into case_coj_doc_block (doc_id,block_html,block_text)
                        values (%s,%s,%s)"""
                val = (doc_id, title_html, str(title_text))
                mycursor.execute(sql, val)
                text_block = title.xpath("./following-sibling::em")[0]
                for p in text_block.xpath("./p"):
                    p_id = ""
                    p_html = etree.tostring(p, pretty_print=True)
                    p_text = p.text_content()
                    leading_text = p_text.split(' ')[0]
                    leading_text = leading_text.strip()
                    ###insert with paragraph id if first word is digit
                    if leading_text.isdigit():
                        p_id = leading_text
                    sql = """insert into case_coj_doc_block (doc_id,block_html,block_text,block_number)
                                values (%s,%s,%s,%s)"""
                    val = (doc_id, p_html, str(p_text), p_id)
                    mycursor.execute(sql, val)
                    print(p_id, p_html)
            mydb.commit()
            continue
        doc_texts = doc_content.xpath("//div[@lang]")
        if len(doc_texts) > 0:
            for doc_text in doc_texts:
                #check document in the latest structure with table wrapping paragraph with countnumber
                counttag = doc_text.xpath('./table/tr/td/p[@class="count"]')
                if len(counttag) > 0:
                    nodes = doc_text.xpath("./child::node()")
                    print(nodes)
                    for p in nodes:
                        try:
                            p_html = etree.tostring(p, pretty_print=True)
                            p_text = p.text_content()
                        except:
                            continue
                        p_id = ""
                        if p.tag == 'table':
                            if len(p.xpath("./col")) > 0:
                                count = p.xpath('./tr/td/p[@class="count"]')
                                if len(count) > 0:
                                    try:
                                        p_id = count[0].text_content()
                                    except:
                                        pass
                        sql = """insert into case_coj_doc_block (doc_id,block_html,block_text,block_number)
                                    values (%s,%s,%s,%s)"""
                        val = (doc_id, p_html, str(p_text), str(p_id))
                        print(p_id, p_html)
                        mycursor.execute(sql, val)
                    continue
                #check document in the structure wrapping countnumber in <a> tag inside <p>
                counttag = doc_text.xpath('./p/a[starts-with(@name,"point")]')
                if len(counttag) > 0:
                    nodes = doc_text.xpath("./child::node()")
                    print(nodes)
                    for p in nodes:
                        try:
                            p_html = etree.tostring(p, pretty_print=True)
                            p_text = p.text_content()
                        except:
                            continue
                        p_id = ""
                        if len(p.xpath('./a[starts-with(@name,"point")]')) > 0:
                            count = p.xpath('./a[starts-with(@name,"point")]')[0]
                            try:
                                p_id = count.text_content()
                            except:
                                pass
                        sql = """insert into case_coj_doc_block (doc_id,block_html,block_text,block_number)
                                    values (%s,%s,%s,%s)"""
                        val = (doc_id, p_html, str(p_text), str(p_id))
                        print(p_id, p_html)
                        mycursor.execute(sql, val)
                    continue
                #check document in the structure wrapping countnumber in <P> tag with class C01PointnumeroteAltN
                counttag = doc_text.xpath('./p[@class="C01PointnumeroteAltN"]')
                if len(counttag) > 0:
                    nodes = doc_text.xpath("./child::node()")
                    print(nodes)
                    for p in nodes:
                        try:
                            p_html = etree.tostring(p, pretty_print=True)
                            p_text = p.text_content()
                        except:
                            continue
                        p_id = ""
                        if p.get('class') == "C01PointnumeroteAltN":
                            p_textstring = p_text.replace('&#160;', ' ')
                            p_textstring = p_textstring.replace('&nbsp;', ' ')
                            p_textstring = re.sub(' +', ' ', p_textstring)
                            p_textstring = re.sub('\s+', ' ', p_textstring)
                            leading_text = p_textstring.split(' ')[0]
                            leading_text = leading_text.strip()
                            if leading_text.isdigit() or (len(leading_text) > 1 and (leading_text[:-1]).isdigit()):
                                p_id = leading_text

                        sql = """insert into case_coj_doc_block (doc_id,block_html,block_text,block_number)
                                    values (%s,%s,%s,%s)"""
                        val = (doc_id, p_html, str(p_text), str(p_id))
                        print(p_id, p_html)
                        mycursor.execute(sql, val)
                    continue
                ##check document in the structure where texts placed in <p> tag with fixed classes inside <div lang> directly but without count
                titlewithclass = doc_text.xpath('./p[@class="C10Titre"]')
                if len(titlewithclass) > 0:
                    nodes = doc_text.xpath("./child::node()")
                    print(nodes)
                    for p in nodes:
                        try:
                            p_html = etree.tostring(p, pretty_print=True)
                            p_text = p.text_content()
                        except:
                            continue
                        p_id = ""
                        if p.tag == 'table':
                            if len(p.xpath('./tr/td[@width="82"]/p')) > 0:
                                count = p.xpath('./tr/td[@width="82"]/p')[0]
                                try:
                                    p_id = count.text_content()
                                    p_id = p_id.stripe()
                                except:
                                    pass
                        sql = """insert into case_coj_doc_block (doc_id,block_html,block_text,block_number)
                                                        values (%s,%s,%s,%s)"""
                        val = (doc_id, p_html, str(p_text), str(p_id))
                        print(p_id, p_html)
                        mycursor.execute(sql, val)
                    continue
                ##check document in the structure where texts placed in <p> tag with fixed classes inside <div lang> directly but without count 2
                titlewithclass = doc_text.xpath('./p[@class="C39Centre"]')
                if len(titlewithclass) > 0:
                    nodes = doc_text.xpath("./child::node()")
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
                    continue
                #####check document in the structure where texts placed in div with class texte tag
                text_block = doc_text.xpath('./div[@class="listNotice"]/div[@class="texte"]')
                if len(text_block) > 0:
                    for doc_text_div in text_block:
                        nodes = doc_text_div.xpath("./child::node()")
                        print(nodes)
                        for p in nodes:
                            try:
                                p_html = etree.tostring(p, pretty_print=True)
                                p_text = p.text_content()
                            except:
                                continue
                            p_id = ""
                            leading_text = p_text.split(' ')[0]
                            leading_text = leading_text.strip()
                            ###insert with paragraph id if first word is digit
                            if leading_text.isdigit() or (len(leading_text) > 1 and (leading_text[:-1]).isdigit()):
                                p_id = leading_text
                            sql = """insert into case_coj_doc_block (doc_id,block_html,block_text,block_number)
                                        values (%s,%s,%s,%s)"""
                            val = (doc_id, p_html, str(p_text), p_id)
                            mycursor.execute(sql, val)
                            print(p_id, p_html)
                    continue
                #####check document in the structure where texts placed in dl tag under font...
                text_in_dl = doc_text.xpath('./font/dl')
                if len(text_in_dl) > 0:
                    nodes = doc_text.xpath("./child::node()")
                    for p in nodes:
                        if hasattr(p, 'tag') and p.tag == 'font':
                            dl_texts = p.xpath("./dl")
                            for dl_text in dl_texts:
                                dl_nodes = dl_text.xpath("./child::node()")
                                for num, dl_p in enumerate(dl_nodes):
                                    try:
                                        p_id = ''
                                        p_html = etree.tostring(dl_p, pretty_print=True)
                                        p_text = dl_p.text_content()
                                        if dl_p.tag != "dt" and dl_p.tag != "dd":
                                            sql = """insert into case_coj_doc_block (doc_id,block_html,block_text,
                                            block_number) values (%s,%s,%s,%s) """
                                            val = (doc_id, p_html, str(p_text), p_id)
                                            mycursor.execute(sql, val)
                                            print(p_id, p_html)
                                    except:
                                        p_html = str(dl_p)
                                        p_textstring = re.sub(' +', ' ', p_html)
                                        p_textstring = re.sub('\s+', ' ', p_textstring)
                                        p_textstring = re.sub('\n+', ' ', p_textstring)
                                        p_textstring = p_textstring.strip()
                                        if p_textstring != "":
                                            try:
                                                p_id = dl_nodes[num-2].text_content()
                                                p_id = re.sub('\s+', ' ', p_id)
                                                p_id = re.sub('\n+', ' ', p_id)
                                                p_id = re.sub(' +', ' ', p_id)
                                                p_id = p_id.strip()
                                                if p_id.isdigit():
                                                    pass
                                                else:
                                                    p_id = ''
                                            except:
                                                p_id = ''
                                            p_text = p_textstring
                                            sql = """insert into case_coj_doc_block (doc_id,block_html,block_text,
                                            block_number) values (%s,%s,%s,%s) """
                                            val = (doc_id, p_html, str(p_text), p_id)
                                            mycursor.execute(sql, val)
                                            print(p_id, p_html)
                        else:
                            try:
                                p_html = etree.tostring(p, pretty_print=True)
                                p_text = p.text_content()
                            except:
                                continue
                            p_id = ""
                            sql = """insert into case_coj_doc_block (doc_id,block_html,block_text,block_number)
                                        values (%s,%s,%s,%s)"""
                            val = (doc_id, p_html, str(p_text), p_id)
                            mycursor.execute(sql, val)
                            print(p_id, p_html)
                    continue
                #####check document in the structure where texts placed in dl tag directly...
                text_in_dl = doc_text.xpath('./dl')
                if len(text_in_dl) > 0:
                    nodes = doc_text.xpath("./child::node()")
                    for p in nodes:
                        if hasattr(p, 'tag') and p.tag == 'dl':
                            dl_text = p
                            dl_nodes = dl_text.xpath("./child::node()")
                            for num, dl_p in enumerate(dl_nodes):
                                p_id = ''
                                try:
                                    p_html = etree.tostring(dl_p, pretty_print=True)
                                    p_text = dl_p.text_content()
                                except:
                                    continue
                                if dl_p.tag != "dt" and dl_p.tag != "dd":
                                    sql = """insert into case_coj_doc_block (doc_id,block_html,block_text,
                                    block_number) values (%s,%s,%s,%s) """
                                    val = (doc_id, p_html, str(p_text), p_id)
                                    mycursor.execute(sql, val)
                                    print(p_id, p_html)
                                if dl_p.tag == "dd":
                                    try:
                                        p_id = dl_nodes[num - 1].text_content()
                                        p_id = p_id.strip()
                                    except:
                                        pass
                                    sql = """insert into case_coj_doc_block (doc_id,block_html,block_text,
                                    block_number) values (%s,%s,%s,%s) """
                                    val = (doc_id, p_html, str(p_text), p_id)
                                    mycursor.execute(sql, val)
                                    print(p_id, p_html)

                        else:
                            try:
                                p_html = etree.tostring(p, pretty_print=True)
                                p_text = p.text_content()
                            except:
                                continue
                            p_id = ""
                            sql = """insert into case_coj_doc_block (doc_id,block_html,block_text,block_number)
                                        values (%s,%s,%s,%s)"""
                            val = (doc_id, p_html, str(p_text), p_id)
                            mycursor.execute(sql, val)
                            print(p_id, p_html)
                    continue
                #####check document in the structure where texts placed in p tag directly...
                for block in list(doc_text):
                    if block.tag in ['p', 'table']:
                        p_html = etree.tostring(block, pretty_print=True)
                        p_text = block.text_content()
                        p_id = ''
                        if block.tag == 'table':
                            p_id_node = block.xpath('.//p[@class="coj-count"]')
                            if len(p_id_node) > 0:
                                p_id_node = p_id_node[0]
                                p_id = p_id_node.text
                        sql = """insert into case_coj_doc_block (doc_id,block_html,block_text,block_number)
                                                            values (%s,%s,%s,%s)"""
                        val = (doc_id, p_html, str(p_text), p_id)
                        mycursor.execute(sql, val)
                        print(p_id, p_text)

            mydb.commit()
            continue






