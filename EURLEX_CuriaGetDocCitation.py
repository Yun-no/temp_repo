import lxml.html as HT
import re
import mysql.connector
import json


def find_html_citation(block_id, html_text, block_text, doc_id, case_grp_id):
    try:
        html_content = HT.fromstring(html_text)
    except:
        return False
    citation_found = 0
    citations = html_content.xpath('//a[@class="CourtLink"]')
    if len(citations) > 0:
        current_case = ''
        current_docnr = ''
        current_paragraph = -1
        for citation in citations:
            ###start loop
            citation_text = citation.text_content()
            citation_text = citation_text.strip()
            case_nr_pattern = re.compile(r'.*\d+/\d+')
            case_nr_match = re.search(case_nr_pattern, citation_text)
            #begin new case citation and store last citation if text match case_nr pattern
            if case_nr_match:
                if current_case != '':
                    insert_sql = """INSERT INTO case_coj_doc_citation (block_id, doc_id, case_grp_id, cited_case_text, 
                    cited_case_docnr, cited_paragraph) VALUES (%s, %s, %s, %s, %s, %s) """
                    insert_val = (block_id, doc_id, case_grp_id, current_case, current_docnr, str(current_paragraph))
                    mycursor.execute(insert_sql, insert_val)
                    print('store %s doc_nr %s paragraph %s' % (current_case, current_docnr, current_paragraph))
                    citation_found = 1
                current_case = citation_text
                current_docnr = ''
                current_paragraph = -1
                continue
            doc_nr_pattern = re.compile(r'.*:.*:.*:.*')
            doc_nr_match = re.search(doc_nr_pattern, citation_text)
            #attach to current case citation if text match doc_nr pattern
            if doc_nr_match:
                current_docnr = citation_text
                continue
            paragraph_pattern = re.compile(r'\d+')
            paragraph_match = re.match(paragraph_pattern, citation_text)
            if paragraph_match:
                # if first paragraph id - set value as current paragraph
                if current_paragraph == -1:
                    current_paragraph = str(citation_text)
                    continue
                # if has paragraph id before, check text between the 2 numbers
                else:
                    #build regex research patten with CASE_NR text + any + previous paragraph id + text to match + current paragraph id
                    paragraph_connectword_pattern = re.compile(current_case + r'.*'+current_paragraph + r'(.*)'+citation_text)
                    paragraph_connectword_match = re.search(paragraph_connectword_pattern, block_text)
                    if paragraph_connectword_match:
                        paragraph_connectword = paragraph_connectword_match.group(1)
                        paragraph_connectword = paragraph_connectword.strip()
                        # text is "to" - store all paragraph numbers in between the 2 number as citation record
                        if paragraph_connectword.lower() == "to":
                            #if new paragraph number smaller than the last one. Sth is wrong. Raise Error.
                            from_p_id = int(current_paragraph)
                            to_p_id = int(citation_text)
                            if to_p_id < from_p_id:
                                raise ValueError("Can't store paragraph range from %d to %d of case %s from text %s"%(from_p_id, to_p_id, current_case, block_text))
                            current_paragraph = current_paragraph + "," + ",".join(str(i) for i in range(from_p_id+1, to_p_id+1))
                            continue
                        # else (text is "and" or other stuff) store previous paragraph as citation record
                        else:
                            current_paragraph = current_paragraph + "," +citation_text
                            continue
                    else:
                        current_paragraph = current_paragraph + "," +citation_text
                        continue
        #store last one
        if current_case != '':
            insert_sql = """INSERT INTO case_coj_doc_citation (block_id, doc_id, case_grp_id, cited_case_text, 
            cited_case_docnr, cited_paragraph) VALUES (%s, %s, %s, %s, %s, %s) """
            insert_val = (block_id, doc_id, case_grp_id, current_case, current_docnr, str(current_paragraph))
            mycursor.execute(insert_sql, insert_val)
            print('store %s doc_nr %s paragraph %s' % (current_case, current_docnr, current_paragraph))
            citation_found = 1
    if citation_found == 1:
        return True
    else:
        return False
        #print(citation.text_content())

def find_html_citation2(block_id, html_text, block_text, doc_id, case_grp_id):
    try:
        html_content = HT.fromstring(html_text)
    except:
        return False
    citation_found = 0
    citations = html_content.xpath('//a[contains(@href,"http://eur-lex.europa.eu/query.html?")]')
    if len(citations) > 0:
        current_case = ''
        current_docnr = ''
        current_paragraph = -1
        for citation in citations:
            ###start loop
            citation_text = citation.text_content()
            citation_text = citation_text.strip()
            case_nr_pattern = re.compile(r'.*\d+/\d+')
            case_nr_match = re.findall(case_nr_pattern, citation_text)
            #begin new case citation and store last citation if text match case_nr pattern
            case_nr = ''
            if len(case_nr_match) > 0:
                case_nr = ','.join(case_nr_match)
            doc_nr_pattern = re.compile(r'ECR\s.*')
            doc_nr_match = re.findall(doc_nr_pattern, citation_text)
            doc_nr = ''
            if len(doc_nr_match) > 0:
                doc_nr = doc_nr_match[0]
            following_text = citation.xpath('./following-sibling::text()[1]')
            paragraph_nr = -1
            try:
                following_text = following_text[0]
                paragraph_pattern = re.compile(r'paragraph\s\d+[\s,)]')
                paragraph_match = re.findall(paragraph_pattern, following_text)
                paragraph_nr = paragraph_match[0]
                paragraph_nr = ''.join(filter(str.isdigit, paragraph_nr))
            except:
                pass
            if case_nr != '' or doc_nr != '':
                insert_sql = """INSERT INTO case_coj_doc_citation (block_id, doc_id, case_grp_id, cited_case_text, 
                            cited_case_docnr, cited_paragraph, citation_text) VALUES (%s, %s, %s, %s, %s, %s, %s) """
                insert_val = (block_id, doc_id, case_grp_id, case_nr, doc_nr, str(paragraph_nr),citation_text)
                mycursor.execute(insert_sql, insert_val)
                print('store %s doc_nr %s paragraph %s' % (case_nr, doc_nr, paragraph_nr))
                citation_found = 1
    if citation_found == 1:
        return True
    else:
        return False



def find_text_citation(block_id, html_text, block_text, doc_id, case_grp_id):
    try:
        if type(block_text) is bytes:
            block_text = block_text.decode()
        block_text = re.sub(' +', ' ',block_text)
        block_text = block_text.replace('\r','')
        block_text = block_text.replace('\n', '')
        citation_pattern = re.compile(r'\([^()]*?\d+/\d+.*?:.*?:.*?:[^)]*\)')

        citations = re.findall(citation_pattern, block_text)
    except:
        return False
    current_paragraph = -1
    citation_found = 0
    for citation in citations:
        #print(citation)
        case_pattern = re.compile(r'..\d+/\d+.*?,*?:.*?:.*?:\d+')
        case_match = re.findall(case_pattern, citation)
        for cast_text in case_match:
            case_nr_patten = re.compile(r'..\d+/\d+.*?,')
            case_nr_match = re.search(case_nr_patten, cast_text)
            if case_nr_match:
                case_nr = case_nr_match.group(0)
                case_nr = case_nr.lstrip(',')
                case_nr = case_nr.lstrip('(')
                case_nr = case_nr.lstrip(')')
                case_nr = case_nr.rstrip(',')
                #print(case_nr)
                doc_nr = ''
                doc_nr_patten = re.compile(r'EU:.*?:.*?:\d+')
                doc_nr_match = re.search(doc_nr_patten, cast_text)
                if doc_nr_match:
                    doc_nr = doc_nr_match.group(0)
                    #print(doc_nr)
                insert_sql = """INSERT INTO case_coj_doc_citation (block_id, doc_id, case_grp_id, cited_case_text, 
                cited_case_docnr, cited_paragraph,citation_text) VALUES (%s, %s, %s, %s, %s, %s, %s) """
                insert_val = (block_id, doc_id, case_grp_id, case_nr, doc_nr, current_paragraph, citation)
                mycursor.execute(insert_sql, insert_val)
                print('store %s doc_nr %s' % (case_nr, doc_nr))
                citation_found = 1
        if len(case_match) == 0:
            case_pattern2 = re.compile(r'..\d+/\d+.*?:.*?:.*?:\d+')
            case_match2 = re.findall(case_pattern2, citation)
            for cast_text2 in case_match2:
                case_nr_patten2 = re.compile(r'..\d+/\d+.*?')
                case_nr_match2 = re.search(case_nr_patten2, cast_text2)
                if case_nr_match2:
                    case_nr = case_nr_match2.group(0)
                    case_nr = case_nr.lstrip(',')
                    case_nr = case_nr.lstrip('(')
                    case_nr = case_nr.lstrip(')')
                    case_nr = case_nr.rstrip(',')
                    doc_nr = ''
                    doc_nr_patten = re.compile(r'EU:.*?:.*?:\d+')
                    doc_nr_match = re.search(doc_nr_patten, cast_text2)
                    if doc_nr_match:
                        doc_nr = doc_nr_match.group(0)
                        #print(doc_nr)
                    insert_sql = """INSERT INTO case_coj_doc_citation (block_id, doc_id, case_grp_id, cited_case_text, 
                    cited_case_docnr, cited_paragraph,citation_text) VALUES (%s, %s, %s, %s, %s, %s, %s) """
                    insert_val = (block_id, doc_id, case_grp_id, case_nr, doc_nr, current_paragraph, citation)
                    mycursor.execute(insert_sql, insert_val)
                    print('store %s doc_nr %s' % (case_nr, doc_nr))
                    citation_found = 1
    citation_pattern = re.compile(r'\[[^\[\])]*?\d+/\d+.*?:.*?:.*?:[^\]]*\]')
    citations = re.findall(citation_pattern, block_text)
    for citation in citations:
        #print(citation)
        case_pattern = re.compile(r'..\d+/\d+.*?,*?:.*?:.*?:\d+')
        case_match = re.findall(case_pattern, citation)
        for cast_text in case_match:
            case_nr_patten = re.compile(r'..\d+/\d+.*?,')
            case_nr_match = re.search(case_nr_patten, cast_text)
            if case_nr_match:
                case_nr = case_nr_match.group(0)
                case_nr = case_nr.lstrip(',')
                case_nr = case_nr.lstrip('(')
                case_nr = case_nr.lstrip(')')
                case_nr = case_nr.rstrip(',')
                #print(case_nr)
                doc_nr = ''
                doc_nr_patten = re.compile(r'EU:.*?:.*?:\d+')
                doc_nr_match = re.search(doc_nr_patten, cast_text)
                if doc_nr_match:
                    doc_nr = doc_nr_match.group(0)
                    #print(doc_nr)
                insert_sql = """INSERT INTO case_coj_doc_citation (block_id, doc_id, case_grp_id, cited_case_text, 
                cited_case_docnr, cited_paragraph,citation_text) VALUES (%s, %s, %s, %s, %s, %s, %s) """
                insert_val = (block_id, doc_id, case_grp_id, case_nr, doc_nr, current_paragraph, citation)
                mycursor.execute(insert_sql, insert_val)
                print('store %s doc_nr %s' % (case_nr, doc_nr))
                citation_found = 1
        if len(case_match) == 0:
            case_pattern2 = re.compile(r'..\d+/\d+.*?:.*?:.*?:\d+')
            case_match2 = re.findall(case_pattern2, citation)
            for cast_text2 in case_match2:
                case_nr_patten2 = re.compile(r'..\d+/\d+.*?')
                case_nr_match2 = re.search(case_nr_patten2, cast_text2)
                if case_nr_match2:
                    case_nr = case_nr_match2.group(0)
                    case_nr = case_nr.lstrip(',')
                    case_nr = case_nr.lstrip('(')
                    case_nr = case_nr.lstrip(')')
                    case_nr = case_nr.rstrip(',')
                    doc_nr = ''
                    doc_nr_patten = re.compile(r'EU:.*?:.*?:\d+')
                    doc_nr_match = re.search(doc_nr_patten, cast_text2)
                    if doc_nr_match:
                        doc_nr = doc_nr_match.group(0)
                        #print(doc_nr)
                    insert_sql = """INSERT INTO case_coj_doc_citation (block_id, doc_id, case_grp_id, cited_case_text, 
                    cited_case_docnr, cited_paragraph,citation_text) VALUES (%s, %s, %s, %s, %s, %s, %s) """
                    insert_val = (block_id, doc_id, case_grp_id, case_nr, doc_nr, current_paragraph, citation)
                    mycursor.execute(insert_sql, insert_val)
                    print('store %s doc_nr %s' % (case_nr, doc_nr))
                    citation_found = 1
    if citation_found == 1:
        return True
    else:
        return False

def find_text_citation2(block_id, html_text, block_text, doc_id, case_grp_id):
    try:
        if type(block_text) is bytes:
            block_text = block_text.decode()
        block_text = re.sub(' +', ' ',block_text)
        block_text = block_text.replace('\r','')
        block_text = block_text.replace('\n', '')
        citation_pattern = re.compile(r'\([^()]*?\d+/\d+.*?ECR\s.*?[^)]*\)')
        citations = re.findall(citation_pattern, block_text)
    except:
        return False
    current_paragraph = -1
    citation_found = 0
    for citation in citations:
        case_nr_pattern = re.compile(r'[Cc]ase\s.*?\d+/\d+')
        case_nr_match = re.findall(case_nr_pattern, citation)
        doc_nr_pattern = re.compile(r'ECR\s.*?[,\)]')
        doc_nr_match = re.findall(doc_nr_pattern, citation)
        for i in range(len(case_nr_match)):
            case_nr = case_nr_match[i]
            try:
                doc_nr = doc_nr_match[i]
            except:
                doc_nr = ''
            paragraph_nr = -1
            insert_sql = """INSERT INTO case_coj_doc_citation (block_id, doc_id, case_grp_id, cited_case_text, 
                                        cited_case_docnr, cited_paragraph, citation_text) VALUES (%s, %s, %s, %s, %s, %s, %s) """
            insert_val = (block_id, doc_id, case_grp_id, case_nr, doc_nr, str(paragraph_nr), citation)
            mycursor.execute(insert_sql, insert_val)
            print('store %s doc_nr %s paragraph %s' % (case_nr, doc_nr, paragraph_nr))
            citation_found = 1
    if citation_found == 1:
        return True
    else:
        return False

def find_text_citation3(block_id, html_text, block_text, doc_id, case_grp_id):
    try:
        if type(block_text) is bytes:
            block_text = block_text.decode()
        block_text = re.sub(' +', ' ', block_text)
        block_text = block_text.replace('\r', '')
        block_text = block_text.replace('\n', '')
        citation_pattern = re.compile(r'\([^()]*?[Cc]ase\s.*?\d+/\d+.*?[^)]*\)')
        citations = re.findall(citation_pattern, block_text)
    except:
        return False
    citation_found = 0
    current_paragraph = -1
    for citation in citations:
        doc_nr_pattern1 = re.compile(r'ECR\s.*?')
        doc_nr_match = re.search(doc_nr_pattern1, citation)
        if doc_nr_match:
            continue
        doc_nr_pattern2 = re.compile(r'.*:.*:.*:.*')
        doc_nr_match = re.search(doc_nr_pattern2, citation)
        if doc_nr_match:
            continue
        case_nr_pattern = re.compile(r'[Cc]ase\s.*?\d+/\d+.*?[,\)\s]')
        case_nr_match = re.findall(case_nr_pattern, citation)
        for case_nr in case_nr_match:
            paragraph_nr = -1
            doc_nr = ''
            insert_sql = """INSERT INTO case_coj_doc_citation (block_id, doc_id, case_grp_id, cited_case_text,
            cited_case_docnr, cited_paragraph, citation_text) VALUES (%s, %s, %s, %s, %s, %s, %s) """
            insert_val = (block_id, doc_id, case_grp_id, case_nr, doc_nr, str(paragraph_nr), citation)
            mycursor.execute(insert_sql, insert_val)
            print('store %s doc_nr %s paragraph %s' % (case_nr, doc_nr, paragraph_nr))
            citation_found = 1
    if citation_found == 1:
        return True
    else:
        return False

def find_text_citation4(block_id, html_text, block_text, doc_id, case_grp_id, case_id_digits):
    try:
        if type(block_text) is bytes:
            block_text = block_text.decode()
        block_text = re.sub(' +', ' ', block_text)
        block_text = block_text.replace('\r', '')
        block_text = block_text.replace('\n', '')
        citation_pattern = re.compile(r'C.\d+/\d+')
        citations = re.findall(citation_pattern, block_text)
    except:
        return False
    current_paragraph = -1
    citation_found = 0
    for citation in citations:
        if block_text.find('EU:')>=0:
            continue
        if block_text.find('ECR')>=0:
            continue
        new_case_ref_blocks = citation.split('/')
        new_case_id_digit = ''
        for new_case_ref_block in new_case_ref_blocks:
            new_case_digit = ''.join(filter(str.isdigit, new_case_ref_block))
            if new_case_id_digit == '':
                new_case_id_digit = new_case_digit
            else:
                new_case_id_digit += '|' + new_case_digit
        if new_case_id_digit in case_id_digits:
            #print("self citation %s"%citation)
            continue
        paragraph_nr = -1
        doc_nr = ''
        insert_sql = """INSERT INTO case_coj_doc_citation (block_id, doc_id, case_grp_id, cited_case_text,
        cited_case_docnr, cited_paragraph) VALUES (%s, %s, %s, %s, %s, %s) """
        insert_val = (block_id, doc_id, case_grp_id, citation, doc_nr, str(paragraph_nr))
        mycursor.execute(insert_sql, insert_val)
        print('store %s doc_nr %s paragraph %s' % (citation, doc_nr, paragraph_nr))
        citation_found = 1
    if citation_found == 1:
        return True
    else:
        return False



if __name__ == '__main__':
    mydb = mysql.connector.connect(user='root', password='',
                                   host='127.0.0.1',
                                   database='eurlex_legalacts')
    mycursor = mydb.cursor(dictionary=True)
    mycursor.execute("""SELECT case_coj_doc.id, case_coj_doc.celex_id, case_grp_id, case_coj_grp.case_ref from 
    case_coj_doc inner join case_coj_grp on case_coj_doc.case_grp_id = case_coj_grp.id where 
    case_coj_doc.citation_built = 0 and case_coj_doc.celex_id is not null order by case_coj_doc.id desc""")
    myresult = mycursor.fetchall()

    for doc in myresult:
        doc_id = doc['id']
        case_grp_id = doc['case_grp_id']
        celex_id = doc['celex_id']
        case_ref = doc['case_ref']
        print(case_ref)
        print(celex_id)
        mycursor.execute("""SELECT * from case_coj_doc_block where doc_id=%s""" % doc_id)
        blocks = mycursor.fetchall()
        has_citation = False
        for block in blocks:
            block_id = block['id']
            block_html = block['block_html']
            block_text = block['block_text']
            ##get accurate citations by wrapped in <a> tag with href
            found_citation = find_html_citation(block_id, block_html, block_text, doc_id, case_grp_id)
            if found_citation == True:
                has_citation = True
        print("html citation", has_citation)
        if has_citation:
            sql = "UPDATE case_coj_doc SET citation_built=%s where id = %s"
            ###use 1 as value for checked through html citations on cases
            val = (1, doc_id)
            mycursor.execute(sql, val)
            mydb.commit()
            continue

        for block in blocks:
            block_id = block['id']
            block_html = block['block_html']
            block_text = block['block_text']
            ##get citations by regex
            found_citation = find_html_citation2(block_id, block_html, block_text, doc_id, case_grp_id)
            if found_citation == True:
                has_citation = True
        print("html citation2", has_citation)
        if has_citation:
            sql = "UPDATE case_coj_doc SET citation_built=%s where id = %s"
            ###use 3 as value for checked through extra html citations on cases
            val = (3, doc_id)
            mycursor.execute(sql, val)
            mydb.commit()
            continue

        for block in blocks:
            block_id = block['id']
            block_html = block['block_html']
            block_text = block['block_text']
            ##get citations by regex
            found_citation = find_text_citation(block_id, block_html, block_text, doc_id, case_grp_id)
            if found_citation == True:
                has_citation = True
        print("TEXT citation", has_citation)
        if has_citation:
            sql = "UPDATE case_coj_doc SET citation_built=%s where id = %s"
            ###use 2 as value for checked through regex citations on cases
            val = (2, doc_id)
            mycursor.execute(sql, val)
            mydb.commit()
            continue


        for block in blocks:
            block_id = block['id']
            block_html = block['block_html']
            block_text = block['block_text']
            ##get citations by regex
            found_citation = find_text_citation2(block_id, block_html, block_text, doc_id, case_grp_id)
            if found_citation == True:
                has_citation = True
        print("TEXT citation2", has_citation)
        if has_citation:
            sql = "UPDATE case_coj_doc SET citation_built=%s where id = %s"
            ###use 4 as value for checked through extra text citations on cases
            val = (4, doc_id)
            mycursor.execute(sql, val)
            mydb.commit()
            continue


        for block in blocks:
            block_id = block['id']
            block_html = block['block_html']
            block_text = block['block_text']
            ##get citations by regex
            found_citation = find_text_citation3(block_id, block_html, block_text, doc_id, case_grp_id)
            if found_citation == True:
                has_citation = True
        print("TEXT citation3", has_citation)
        if has_citation:
            sql = "UPDATE case_coj_doc SET citation_built=%s where id = %s"
            ###use 5 as value for checked through general case nr text citations without EUR:* ref and ECR* ref on cases
            val = (5, doc_id)
            mycursor.execute(sql, val)
            mydb.commit()
            continue

        case_ref_list = case_ref.split('|')
        case_id_digits =[]
        for case_ref in case_ref_list:
            case_ref_blocks = case_ref.split('/')
            case_id_digit = ''
            for case_ref_block in case_ref_blocks:
                case_digit = ''.join(filter(str.isdigit, case_ref_block))
                if case_id_digit == '':
                    case_id_digit = case_digit
                else:
                    case_id_digit += '|'+case_digit
            case_id_digits.append(case_id_digit)

        block_text = ''
        for block in blocks:
            block_id = block['id']
            block_html = block['block_html']
            block_text = block['block_text']
            ##get citations by regex
            found_citation = find_text_citation4(block_id, block_html, block_text, doc_id, case_grp_id, case_id_digits)
            if found_citation == True:
                has_citation = True
        print("TEXT citation4", has_citation)
        if has_citation:
            sql = "UPDATE case_coj_doc SET citation_built=%s where id = %s"
            ###use 6 as value for checked through general case nr text citations with most general regex strictly rule out EUR and ECR
            val = (6, doc_id)
            mycursor.execute(sql, val)
            mydb.commit()
            continue

