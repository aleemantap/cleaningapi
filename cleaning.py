import re
import csv, sqlite3
from sqlite3 import Error
import pandas as pd
import io
import base64
import numpy as np
import matplotlib.pyplot as plt
import json
import encode
#import connect_db
#conn = connect_db.connectDB()
#data_abusive_to_list = abusive_dataframe(conn)
#kamus_alay_df = kamus_alay_dataframe(conn)


def abusive_dataframe(conn):
    cursor = conn.cursor()
    select_all = "SELECT word_abusive FROM data_abusive"
    rows_kal = cursor.execute(select_all).fetchall()    
    df = pd.DataFrame(rows_kal)
    pdToList = list(df[0])
    #data_abusive_to_list = ['^'+i for i in pdToList]
    print('tes f')
    data_abusive_to_list = [i for i in pdToList]
    return data_abusive_to_list


def kamus_alay_dataframe(conn):
    select_all2 = "SELECT word_not_standar, word_standar FROM data_kamus_alay"
    dd = pd.read_sql(select_all2, conn)
    #print(list(dd['word_not_standar']))
    gg = {'A' : list(dd['word_not_standar']), 'B' : list(dd['word_standar']) }
    print('tes a')
    df2 = pd.DataFrame(gg)
    return df2


def cleaningText(conn,text_kotor,abusive_dataframe,kamus_alay_dataframe):
    ''' start cleaning string '''
    # regex methode
   
    #sps_c = r"[a-zA-Z0-9]+[\a-zA-Z0-9]+\s"
    sps_c = r"[a-zA-Z0-9]+[\a-zA-Z0-9]"
    x2 = re.findall(sps_c, text_kotor.lower())
    x1 = ''.join(x2)
    sxs = re.sub(r"\buser|\bRt|\bRT|[:]","",x1)
    
    # lst = []
    # lst.append(sxs.strip())
   
    # data_dict = {'data_kotor': lst}
    # p = pd.DataFrame(data_dict)    
    ''' end cleaning string '''
    
    ''' start replace  word abusive to *** '''        
    data_abusive_to_list = abusive_dataframe 
    #text = p.loc[0]['data_kotor']
    text = sxs.strip()
    txtSplit = re.split("\s", text)
    #print("txtSplit, :", txtSplit)
    #list_data_hasil_count_abusive = []
    cnt_abusive = 0
    for x in data_abusive_to_list:
        
        for i in txtSplit:
            if x==i:
                text = text.replace(x,'***')
                cnt_abusive = cnt_abusive + 1 
    
    ''' end replace '''    

    ''' start replace  word no standar to standar  '''    
    kamus_alay_df = kamus_alay_dataframe #(conn)
   
    
    text_clean = text #p.loc[0]['data_kotor'] #p2['data_kotor']
    txtSplit2 = re.split("\s", text_clean)
    cnt_alay = 0
    tc = []
    for x,y in dict(kamus_alay_df.values).items():
        #print(A,B)
        #print(x,y)
        for i in txtSplit2:
            if x==i:
                text_clean = text_clean.replace(x,y)
                cnt_alay = cnt_alay + 1 
                    
                    
    hasil = [text_kotor,text_clean.capitalize(),cnt_abusive,cnt_alay] 
    
    #print(hasil)
    return hasil
   

def cleaning_csvdata(conn,filename,abusive_dataframe,kamus_alay_dataframe):
    cursor = conn.cursor()


    # read csv data tweet with pandas and insert to data_tweet sqlite
    df_tweet2 = pd.read_csv(filename,  encoding='latin-1')

    #df_tweet2 = df_tweet.iloc[0:20]
    df_tweet2.rename(columns={'Tweet': 'tweet_not_clean'}, inplace=True)
    lst_count_cleaning_abusive=[]
    lst_count_cleaning_kamus_alay=[]
    lst_tweet_after_clear=[]
    lst_tweet_not_clean=[]

    for i in df_tweet2['tweet_not_clean']:
        x = cleaningText(conn,i,abusive_dataframe,kamus_alay_dataframe) 
        lst_count_cleaning_kamus_alay.append(x[3])
        lst_count_cleaning_abusive.append(x[2])
        lst_tweet_after_clear.append(x[1])
        lst_tweet_not_clean.append(x[0])


    df2s = pd.DataFrame({ 'tweet_not_clean' : lst_tweet_not_clean,  
                          'tweet_after_clear' : lst_tweet_after_clear,
                          'count_cleaning_abusive' : lst_count_cleaning_abusive,
                          'count_cleaning_kamus_alay' : lst_count_cleaning_kamus_alay
                         })

    cv = df2s.to_sql('data_tweet', conn, if_exists='append', index=False)
    
    rowsQuery = "SELECT Count(*) FROM data_tweet"
    cursor.execute(rowsQuery)
    numberOfRows = cursor.fetchone()[0]
    
    return numberOfRows

def cln_raw(conn,text,abusive_dataframe,kamus_alay_dataframe):
   
    x = cleaningText(conn,text,abusive_dataframe,kamus_alay_dataframe) 
    lst_count_cleaning_kamus_alay = x[3]
    lst_count_cleaning_abusive = x[2]
    lst_tweet_after_clear = x[1]
    lst_tweet_not_clean = x[0]
    
    data = (lst_tweet_not_clean, lst_tweet_after_clear, lst_count_cleaning_abusive, lst_count_cleaning_kamus_alay)
       
    
    sql = ''' INSERT INTO data_tweet(tweet_not_clean,tweet_after_clear,count_cleaning_abusive,count_cleaning_kamus_alay)
              VALUES(?,?,?,?) '''
    cur = conn.cursor()
    cur.execute(sql, data)
    conn.commit()
    return [cur.lastrowid,lst_tweet_after_clear]

#fungsi analisis1
def analisis(conns):
    s = io.BytesIO()
    sql_query = pd.read_sql_query ('''
                                SELECT 
                                    sum(count_cleaning_abusive) as jumlah_clean_abusive,
                                    sum(count_cleaning_kamus_alay) as jumlah_clean_alay
                                FROM data_tweet	
                            ''', conns)

    dfxs = pd.DataFrame(sql_query, columns = ['jumlah_clean_abusive','jumlah_clean_alay'])

    cr2 = dfxs.iloc[0]['jumlah_clean_abusive']
    cr3 = dfxs.iloc[0]['jumlah_clean_alay']

    x = np.array(["sum absv", "sum alay"])
    y = np.array([cr2, cr3])


    #===
    # fig 1
    fig, (ax1, ax2) = plt.subplots(1, 2)
    fig.suptitle('Hasil Cleaning')
    for i in range(len(x)):
        ax1.text(i,y[i],y[i])
    ax1.bar(x, y)
    ax1.set_ylabel('Jumlah')
    ax1.set_xlabel('Kategori Cleaning')

    # fig 2
    an2 = analis2(conns)
    baik = an2[0]
    sedang = an2[1]
    buruk = an2[2]
    x2 = np.array(["Baik","Sedang", "Buruk"])
    y2 = np.array([baik, sedang, buruk])
    for i in range(len(x2)):
        ax2.text(i,y2[i],y2[i])
    ax2.bar(x2, y2, color="orange")
    ax2.set_xlabel('Labeling')
    # ===========================================
    #plt.show()
    plt.savefig(s, format='png', bbox_inches="tight")
    #konversi ke bytecode
    s = base64.b64encode(s.getvalue()).decode("utf-8").replace("\n", "")
    #print(s)
    json_str = json.dumps({
                            'jumlah_clean_abusive' : cr2, 
                            'jumlah_clean_alay' : cr3, 
                            'jumlah_baik' : baik, 
                            'jumlah_sedang' : sedang,
                            'jumlah_buruk' : buruk,
                            'base64_image_chart' : str(s),
                            'keterangan' : 'silahkan copy data base64_image_chart  ke dalam situs (https://codebeautify.org/base64-to-image-converter) untuk mendapatkan gambarnya'
                            },  cls=encode.NpEncoder)

    return json_str
   

def analis2(conn):
    s = io.BytesIO()
    df = pd.read_sql_query ('''    SELECT  * FROM data_tweet ''', conn)

    gh = df.assign(
            total_word=lambda x: x['tweet_after_clear'].str.split().str.len(),
            persen_abusive = lambda x: (x['count_cleaning_abusive']/x['total_word'])*100,
            persen_kamus_alay =  lambda x: (x['count_cleaning_kamus_alay']/x['total_word'])*100,
            jumlah_persen =  lambda x: (x['persen_kamus_alay']+x['persen_abusive']),
            nilai_huruf=lambda x: x['jumlah_persen'].apply(nilai_label),
    ).dropna().sort_values(by=['jumlah_persen'], ascending=False)

    df2 = gh.groupby(['nilai_huruf'])['nilai_huruf'].count()
    baik = df2['Baik']
    sedang  =df2['Sedang']
    buruk = df2['Buruk']
    
    return [baik,sedang,buruk]

def nilai_label(x):
    if x>=90 and x <=100:
        return 'Buruk' 
    elif x>10 and x<90:
        return 'Sedang' 
    elif x>=0 and x <=10:
        return 'Baik' 
        
        
    
