import mysql.connector
import json


if __name__ == '__main__':
    keywords = {
        ' EU trade mark ': 166,
        ' Article 181 of the Rules of Procedure of the Court of Justice ': 157,
        ' State aid ': 123,
        ' Community trade mark ': 116,
        ' Agreements, decisions and concerted practices ': 114,
        ' Competition ': 104,
        ' Regulation (EC) No 207/2009 ': 95,
        ' Directive 2006/112/EC ': 76,
        ' Opposition proceedings ': 65,
        ' Obligation to state reasons ': 64,
        ' Action for annulment ': 57,
        ' Likelihood of confusion': 51,
        ' Article 181 of the Rules of Procedure of the Court ': 50,
        ' Admissibility': 43,
        ' Environment ': 42,
        ' Invalidity proceedings ': 42,
        ' Manifest inadmissibility': 41,
        ' VAT ': 39,
        ' Dumping ': 38,
        ' Scope ': 37,
        ' Freedom of establishment ': 34,
        ' Regulation (EC) No 40/94 ': 33,
        ' Fines ': 32,
        ' Freedom to provide services ': 32,
        ' Article 8(1)(b) ': 31,
        ' Charter of Fundamental Rights of the European Union ': 30,
        ' Conditions': 29,
        ' Equal treatment': 29,
        ' Taxation ': 28,
        ' Inadmissibility ': 28,
        ' Principle of proportionality': 25,
        ' Regulation (EC) No 1/2003 ': 25,
        ' Proportionality': 25,
        ' Appeal manifestly unfounded': 25,
        ' Burden of proof': 23,
        ' Unlimited jurisdiction': 22,
        ' Refusal of registration ': 22,
        ' Civil service ': 22,
        ' Urgency': 21,
        ' Rights of the defence ': 20,
        ' Article 181 of the Rules of Procedure ': 20,
        ' Appeal in part manifestly inadmissible and in part manifestly unfounded': 19,
        ' Appeal manifestly inadmissible': 19,
        ' Principle of equal treatment ': 19,
        ' No need to adjudicate': 19,
        ' Community trade mark ': 19,
        ' Regulation (EC) No 1049/2001 ': 19,
        ' Rejection of the application for registration': 19,
        ' Relative ground for refusal ': 18,
        ' Intervention ': 18,
        ' Regulation (EC) No 207/2009 ': 18,
        ' Actions for annulment ': 18,
        ' National legislation ': 17,
        ' Validity ': 17,
        ' Article 8(1)(b) ': 17,
        ' Relative grounds for refusal ': 17,
        ' Directive 91/271/EEC ': 17,
        ' Principle of legal certainty ': 16,
        ' None ': 16,
        ' Justification ': 16,
        ' Free movement of capital ': 16,
        ' Transport ': 16,
        ' Order for interim measures ': 16,
        ' Value added tax ': 15,
        ' Absolute ground for refusal ': 15,
        ' Application for interim measures ': 15,
        ' Declaration of invalidity ': 15,
        ' Regulation (EC) No 44/2001 ': 15,
        ' Non-contractual liability of the European Union ': 15,
        ' Actions for damages ': 15,
        ' Consumer protection ': 15,
        ' Locus standi ': 15,
        ' Directive 1999/70/EC ': 14,
        ' Principle of non-discrimination': 14,
        ' Common foreign and security policy ': 14,
        ' Legal basis': 14,
        ' Directive 92/43/EEC ': 14,
        ' Directive 2004/18/EC ': 14,
        ' Action for damages ': 14,
        ' Duty to state reasons': 14,
        ' Statement of reasons ': 14,
        ' Revocation proceedings ': 14,
        ' Descriptive character ': 13,
        ' Directive 2004/38/EC ': 13,
        ' Rejection of the opposition ': 13,
        ' Manifest error of assessment ': 13,
        ' Article 107(1) TFEU ': 13,
        ' Regulation (EEC) No 1408/71 ': 12,
        ' Betting and gaming ': 12,
        ' Whether permissible ': 12,
        ' Regulation (EC) No 40/94 ': 12,
        ' Article 7(1)(b) ': 12,
        ' Interest in bringing proceedings ': 12,
        ' Concept of ‘State aid’ ': 12,
        ' Regulation (EC) No 1225/2009 ': 12,
        ' Bathroom fittings and fixtures markets of Belgium, Germany, France, Italy, the Netherlands and Austria ': 12,
        ' Article 7 ': 11,
        ' Public service contracts ': 11,
        ' Regulation (EC) No 1/2003 ': 11
    }

    keyword_ids = [5, 6, 10, 8, 3, 28, 40, 11, 31, 20, 26, 35, 18, 68, 73, 78, 84, 97]

    mydb = mysql.connector.connect(user='root', password='',
                                   host='127.0.0.1',
                                   database='curia')
    mycursor = mydb.cursor(dictionary=True)

    i = 1
    case_nr_list = []
    for keyword in keywords.keys():
        if i in keyword_ids:
            mycursor.execute("""SELECT case_nr_origin, case_id, case_nr_cited, cited_case_id, (select count(*) from 
            case_coj_document_citation b where b.cited_case_id=a.cited_case_id) as cited_times FROM 
            case_coj_document_citation a where a.origin_and_cited_exist = 1 and a.cited_case_id in (select distinct case_id from 
            case_coj_keywords where keyword = '%s' )""" % keyword)
            myresult = mycursor.fetchall()
            for result in myresult:
                if result['cited_case_id'] not in case_nr_list:
                    case_nr_list.append(result['cited_case_id'])
                if result['case_id'] not in case_nr_list:
                    case_nr_list.append(result['case_id'])
            mycursor.execute("""SELECT case_nr, id from 
                    case_coj where id in (select distinct case_id from 
                    case_coj_keywords where keyword = '%s' )""" % keyword)
            myresult = mycursor.fetchall()
            for result in myresult:
                if result['id'] not in case_nr_list:
                    case_nr_list.append(result['id'])
        i += 1

    for case_nr in case_nr_list:
        mycursor.execute("""SELECT * from 
                        case_coj where id = %d """ % case_nr)
        myresult = mycursor.fetchall()
        case = myresult[0]
        mycursor.execute("""SELECT id,document_ref,document_nr,title,celex_url from 
                            case_coj_document where case_id = %d """ % case_nr)
        doc_list = []
        myresult = mycursor.fetchall()
        for result in myresult:
            doc_list.append(result)
        case['doclist'] = doc_list
        mycursor.execute("""SELECT case_nr_cited,cited_case_id from 
                                case_coj_document_citation where case_id = %d and cited_case_id is not null""" % case_nr)
        citation_list = []
        myresult = mycursor.fetchall()
        for result in myresult:
            citation_list.append(result)
        case['citationlist'] = citation_list

        with open('case_%d.json' % case_nr, 'w') as fp:
            json.dump(case, fp)
