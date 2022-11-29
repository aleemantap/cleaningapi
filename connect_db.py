# Import required modules
import re
import csv, sqlite3
from sqlite3 import Error
import pandas as pd

def connectDB():
    # Connecting to the geeks database
    #conn = sqlite3.connect('database_tugas_chapter3.db')
    #return conn

    conn = None
    try:
        conn = sqlite3.connect('databasechapter3.db')
    except Error as e:
         print(e)

    return conn

#create db 
def create_db(conn):
    
    #create cursor
    cursor = conn.cursor()

    # Crete Table data tweet
    create_table_dt_tweet = '''
                    CREATE TABLE IF NOT EXISTS data_tweet(
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    tweet_not_clean TEXT NOT NULL,
                    tweet_after_clear TEXT NULL,
                    count_cleaning_abusive NULL,
                    count_cleaning_kamus_alay NULL
                    );
                    '''
    cursor.execute(create_table_dt_tweet)

    # Create Table abusive
    create_table_dt_abusive = '''CREATE TABLE IF NOT EXISTS data_abusive(
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    word_abusive TEXT NOT NULL);
                    '''
    cursor.execute(create_table_dt_abusive)

    # Create Table kamus_alay()
    create_table_dt_kamus_alay = '''CREATE TABLE IF NOT EXISTS data_kamus_alay(
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    word_not_standar TEXT  NOT NULL,
                    word_standar TEXT  NOT NULL
                    );
                    '''
    cursor.execute(create_table_dt_kamus_alay)
    conn.commit()

#form_insert_db_kamus_alay
def form_insert_db_kamus_alay(conn,kata_tidak_standard,kata_standard):
  
    """
    Create a row kamus alay
    param kata_tidak_standard:
    param kata_standard:
  
    """
    form = (kata_tidak_standard,kata_standard)
        

    sql = ''' INSERT INTO data_kamus_alay('word_not_standar','word_standar') VALUES(?,?) '''

    cur = conn.cursor()
    cur.execute(sql, form)
    conn.commit()
    return cur.lastrowid

#form_insert_db_abusive
def form_insert_db_abusive(conn,word):
  
    """
    Create a row abusive
    param : text
   
    """
    form = (word)

    #sql = "INSERT INTO data_abusive(word_abusive)VALUES('1','word')"
    sql = "INSERT INTO data_abusive ('word_abusive') VALUES ('"+word+"')"
    #sql = ''' INSERT INTO data_abusive('word_abusive') VALUES(?) '''
    cur = conn.cursor()
    cur.execute(sql)
    conn.commit()
    #select_all = "SELECT * FROM data_abusive"
    #rowsa = cur.execute(select_all).fetchall() 
    #print(cur.lastrowid)
    return cur.lastrowid



# abusive function tabel action
def insert_data_from_csv_to_db_data_abusive(conn,file_name):
    
    cursor = conn.cursor()
    # read csv data tweet with pandas and insert to data abusvie sqlite
    df_word_abusive = pd.read_csv(file_name,  encoding='latin-1')
  
    df_word_abusive.rename(columns={'ABUSIVE': 'word_abusive'}, inplace=True)
    df_word_abusive['id'] =  [i+1 for i in range(len(df_word_abusive))]
    
    df_word_abusive_new = df_word_abusive[['id','word_abusive']]
    df_word_abusive_new.to_sql("data_abusive",conn,if_exists='replace', index=False)
    #df_word_abusive.to_sql("data_abusive",conn, if_exists='replace', index=False)
    
    select_all = "SELECT * FROM data_abusive"
    rowsa = cursor.execute(select_all).fetchall()    
    #print(rowsa)
    return rowsa


def insert_data_from_csv_to_db_data_kamus_alay(conn,file_name):
    
    cursor = conn.cursor()
    # read csv data tweet with pandas and insert to kamus alay sqlite 
    # karna tidak ada nam kolom didalm csv maka perlu ditambhkn header=None
    df_alay = pd.read_csv(file_name, header=None,  encoding='latin-1')
    df_alay_n = df_alay
    #print(len(df_alay))
    #print(df_alay)
    
    df_alay_n['id'] =  [i+1 for i in range(len(df_alay))]
    df_alay_n['word_not_standar'] =  df_alay[0]
    df_alay_n['word_standar'] =  df_alay[1]
    df_alay_new = df_alay_n[['id','word_not_standar','word_standar']]
   
    df_alay_new.to_sql("data_kamus_alay",conn,if_exists='replace', index=False)
    
    select_all = "SELECT * FROM data_kamus_alay"
    rowsk = cursor.execute(select_all).fetchall()    
    #print(rowsk)
    return rowsk

# data tweet function action
def insert_data_from_csv_to_db_data_tweet(conn):
    
    cursor = conn.cursor()
    # read csv data tweet with pandas and insert to data_tweet sqlite
    df_tweet = pd.read_csv('data.csv', header=None, encoding='latin-1')
    df_tweet_r = df_tweet['Tweet']
  
    df_tweet.rename(columns={'Tweet': 'tweet_not_clean'}, inplace=True)
    df_tweet['id'] =  [i+1 for i in range(len(df_tweet))]
    df_tweet['tweet_after_cler'] = ""  #['' for i in range(len(df_tweet))]
    df_tweet_new = df_tweet[['id','tweet_not_clen','tweet_after_clear']]
    #print(df_tweet_new)
    df_tweet_new.to_sql("data_tweet",conn,if_exists='replace', index=False)
    #print(df_tweet)

    select_all = "SELECT * FROM data_tweet"
    rows_twt = cursor.execute(select_all).fetchall()    
    #print(rows_twt)
    # for r in rows_twt:
    #      print(r)
    
    # Committing the changes
    conn.commit()
    # closing the database connection
    conn.close()

    return rows_twt


# data tweet function action 2
def insert_data_from_csv_to_db_data_tweet2(conn):
    cursor = conn.cursor()
    # read csv data tweet with pandas and insert to data_tweet sqlite
    df_tweet = pd.read_csv('data.csv', header=None, encoding='latin-1')
    df_tweet_r = df_tweet['Tweet']
  
    df_tweet.rename(columns={'Tweet': 'tweet_not_clean'}, inplace=True)
    df_tweet['id'] =  [i+1 for i in range(len(df_tweet))]
    df_tweet['tweet_after_clear'] = ""  #['' for i in range(len(df_tweet))]
    df_tweet_new = df_tweet[['id','tweet_not_clen','tweet_after_clear']]
    
    return  1 #rows_twt

def cleaning_data_upload(conn):
    cursor = conn.cursor()
    select_all = "SELECT word_abusive FROM data_abusive"
    rows_kal = cursor.execute(select_all).fetchall()    
    df = pd.DataFrame(rows_kal)
    pdToList = list(df[0])

    select_all2 = "SELECT word_not_standar, word_standar FROM data_kamus_alay"
    rows_kal2 = cursor.execute(select_all2).fetchall()    
    df2 = pd.DataFrame(rows_kal2)
    
    x = '' #re.sub("[0-9]|^-|[!]", "", text_kotor)
    
    #p = convert_text_to_dataframe(x)
    data_dict = None
    data_dict = {'data_kotor': [x]}
    p = pd.DataFrame(data_dict)    
    p.replace(regex=pdToList, value='***')
    p.replace(regex=dict(df2.values))
    return p

def cleaning_raw_text(conn,text_kotor):
    cursor = conn.cursor()
    #print(data_no_std)
    #print(data_no_std)
    select_all = "SELECT word_abusive FROM data_abusive"
    rows_kal = cursor.execute(select_all).fetchall()    
    df = pd.DataFrame(rows_kal)
    pdToList = list(df[0])
    data_abusive_to_list = ['^'+i for i in pdToList]
    #ks = ['^'+i for i in pdToList]
    #print(rowsa)
    #rows_dab
    select_all2 = "SELECT word_not_standar, word_standar FROM data_kamus_alay"
    #rows_kal2 = cursor.execute(select_all2).fetchall()    
   
    #df2 = pd.DataFrame(rows_kal2)
    #print("rows_kal2 =", rows_kal2)
    dd = pd.read_sql(select_all2, conn)
    gg = {'word_not_standar' : list(dd['word_not_standar']), 'word_standar' : list(dd['word_standar']) } #dict(list(dd['word_not_standar']),list(dd['word_standar']))
    #print(ggg)
    df2 = pd.DataFrame(gg)
   
    x =  re.sub("[0-9]|^-|[!]|[-$]|['$]", "", text_kotor) #re.sub("[0-9]|^-|[!]", "", text_kotor)
    lst = []
    lst.append(x.strip())
    #p = convert_text_to_dataframe(x)
    data_dict = None
    data_dict = {'data_kotor': lst}
    p = pd.DataFrame(data_dict)    
    p2 = p.replace(regex=data_abusive_to_list, value='***')
    #dx = 
    dx = p2.replace(regex=dict(df2.values))
    form = (text_kotor,dx['data_kotor'][0]) 
    
    #print(p2)
    #print(dx)

    sql = ''' INSERT INTO data_tweet('tweet_not_clean','tweet_after_clear') VALUES(?,?) '''
   
    cursor.execute(sql, form)
    conn.commit()
    conn.close()
    return [cursor.lastrowid,dx['data_kotor'][0].strip()]


if  __name__ == "__main__":
    conn = connectDB()
    #insert_data_from_csv_to_db_data_tweet(conn)
    #insert_data_from_csv_to_db_data_abusive(conn,'abusive.csv')
    #insert_data_from_csv_to_db_data_kamus_alay(conn,'new_kamusalay.csv')
    #form_insert_db_abusive(conn,'tess asasda as')
    #form_insert_db_kamus_alay(conn,"aas","kkk")
    #insert_data_from_csv_to_db_data_abusive2(conn,'abusive.csv')
    cleaning_raw_text(conn,"- alay disaat semua cowok berusaha melacak perhatian gue. loe lantas remehkan perhatian yg gue kasih khusus - ke elo. basic 2 elo cowok bego ! ! !'")