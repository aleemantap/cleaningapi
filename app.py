# Import required modules
import re
from flask import Flask, jsonify
from flask import request
from flasgger import Swagger, LazyString, LazyJSONEncoder
from flasgger import swag_from

#import function connect db sqlite
import connect_db
import cleaning
import pandas as pd
import os
from os.path import join, dirname, realpath
import json

app =  Flask(__name__)


app.json_encoder = LazyJSONEncoder
swagger_template = dict(
        info = {
            'title' : LazyString(lambda : 'API Documentation Ali Romli Chapter 3 Chalange '),
            'version' : LazyString(lambda : '1.0.0'),
            'description' : LazyString(lambda : 'API Documentation Ali Romli Chapter 3 Chalange'),
        },
        host = LazyString(lambda:request.host),

    )


swagger_config = {
        "headers" : [],
        "specs" : [
            {
                "endpoint" : 'docs',
                "route" : '/docs.json',
            }
        
        ],
        "static_url_path" : "/flasgger_static",
        "swager_ui" : True,
        "specs_route" : "/docs/",
       
       
    }

# Upload folder
#UPLOAD_FOLDER = 'docs/files'
#app.config['UPLOAD_FOLDER'] =  UPLOAD_FOLDER
swagger = Swagger(app, template=swagger_template, config=swagger_config)

#End point upload abusive csv
@swag_from("yml/upload_csv_abusive.yml", methods=['POST'])
@app.route('/upload-csv-abusive', methods=['POST'])
def upload_csv_abusiv():
    
    json_response={}
    uploaded_file = request.files['upfile']
    if uploaded_file.filename != '':
        
        #remove file duplicate data abusive  csv
        #os.remove(uploaded_file.filename)

        # save the file
        uploaded_file.save(uploaded_file.filename)
      
        #call function connect db and call abusive crete db
        conn = connect_db.connectDB()
        connect_db.create_db(conn)  
        data = connect_db.insert_data_from_csv_to_db_data_abusive(conn,uploaded_file.filename)
       
        if(len(data)>0):
            json_response = {
                'status_code' : 200,
                'description' : "CSV success inserted to DB",
                'data' : 'Ok'
            }
        else:
            json_response = {
                'status_code' : 201,
                'description' : "CSV  failed inserted. Please inserted",
                'data' : 'Try more'
            }

    else: 
        json_response = {
                'status_code' : 400,
                'description' : "CSV filed upload",
                'data' : 'Try more'
         }
        
    
    response_data = jsonify(json_response)
    return response_data

#End point form insert data abusive with raw text
@swag_from("yml/form_insert_data_abusive.yml", methods=['POST'])
@app.route('/form_insert_data_abusive', methods=['POST'])
def form_insert_data_abusive():

    text = request.form.get('text').lower().strip()
  
    conn = connect_db.connectDB()
    if(conn==None):
        json_response = {
            'status_code' : 400,
            'description' : "Koneksi DB gagal Silahkan coba lagi"
        }
    else:    
        connect_db.create_db(conn) # jika tidak ada maka dibuat dulu db nya
        data = connect_db.form_insert_db_abusive(conn,text)
        if(data>0):
        
            json_response = {
                'status_code' : 200,
                'description' : "Insert sudah diproses",
                 #'data' : kata_tidak_standard+"-"+kata_standard
            }
        else:
             json_response = {
                'status_code' : 400,
                'description' : "Insert gagal diproses",
                #'data' : kata_tidak_standard+"-"+kata_standard
            }
    
    response_data = jsonify(json_response)
    return response_data

#End point upload kamus alay csv
@swag_from("yml/upload_csv_kamusalay.yml", methods=['POST'])
@app.route('/upload-csv-kamus-alay', methods=['POST'])
def upload_csv_kamus_alay():
    
    json_response={}
    text = request.form.get('replace')
    uploaded_file = request.files['upfile']
    if uploaded_file.filename != '':
        
        #remove file duplicate data abusive  csv
        # save the file
        uploaded_file.save(uploaded_file.filename)
        #call function connect db and call abusive crete db
        conn = connect_db.connectDB()
        connect_db.create_db(conn)  
        data = connect_db.insert_data_from_csv_to_db_data_kamus_alay(conn,uploaded_file.filename)
       
        if(len(data)>0):
            json_response = {
                'status_code' : 200,
                'description' : "CSV success inserted to DB",
            }
        else:
            json_response = {
                'status_code' : 400,
                'description' : "CSV  failed inserted. Please inserted",
            }

    else: 
        json_response = {
                'status_code' : 200,
                'description' : "CSV filed upload",
         }
        
    
    response_data = jsonify(json_response)
    return response_data

#End point form insert data kamus alay with raw text
@swag_from("yml/form_insert_data_kamus_alay.yml", methods=['POST'])
@app.route('/form_insert_data_kamus_alay', methods=['POST'])
def form_insert_data_kamus_alay():

    kata_tidak_standard = request.form.get('kata_tidak_standard').lower().strip()
    kata_standard = request.form.get('kata_standard').lower().strip()

    #call function connect db and call abusive crete db
    conn = connect_db.connectDB()
    if(conn==None):
        json_response = {
            'status_code' : 400,
            'description' : "Koneksi DB gagal Silahkan coba lagi"
        }
    else:    
        connect_db.create_db(conn) # jika tidak ada maka dibuat dulu db nya
        data = connect_db.form_insert_db_kamus_alay(conn,kata_tidak_standard,kata_standard)
        if(data>0):
        
            json_response = {
                'status_code' : 200,
                'description' : "Insert sudah diproses",
                 #'data' : kata_tidak_standard+"-"+kata_standard
            }
        else:
             json_response = {
                'status_code' : 400,
                'description' : "Insert gagal diproses",
                #'data' : kata_tidak_standard+"-"+kata_standard
            }

    
    response_data = jsonify(json_response)
    return response_data

#End point cleansing text form
@swag_from("yml/text_processing.yml", methods=['POST'])
@app.route('/text-processing-cleaning', methods=['POST'])
def text_processing_cleaning():

    text = request.form.get('text').strip()
    conn = connect_db.connectDB()
    abusive_dataframe = cleaning.abusive_dataframe(conn) 
    kamus_alay_dataframe = cleaning.kamus_alay_dataframe(conn) 
    lid  =  cleaning.cln_raw(conn,text,abusive_dataframe,kamus_alay_dataframe)
    
    if lid[0] > 0:
        json_response = {
            'status_code' : 200,
            'description' : "Text yang sudah diproses",
            'data_kotor' :  text,
            'data_bersih' : lid[1]
        }
    else:
        json_response = {
            'status_code' : 200,
            'description' : "Text Gagal diproses",
            'data_kotor' :  text
        }
    
    response_data = jsonify(json_response)
    return response_data

      
#End point upload data csv
@swag_from("yml/upload_csv_data_tweet.yml", methods=['POST'])
@app.route('/upload-data-tweet', methods=['POST'])
def upload_csv_data_tweet():
    
    json_response={}
    text = request.form.get('replace')
    uploaded_file = request.files['upfile']
    if uploaded_file.filename != '':
        
        # save the file
        uploaded_file.save(uploaded_file.filename)
        
        #call function connect db and call abusive crete db
        conn = connect_db.connectDB()
        connect_db.create_db(conn)  
        abusive_dataframe = cleaning.abusive_dataframe(conn) 
        kamus_alay_dataframe = cleaning.kamus_alay_dataframe(conn) 
        data = cleaning.cleaning_csvdata(conn,uploaded_file.filename,abusive_dataframe,kamus_alay_dataframe)

        if data > 0:
                #cx = connect_db.cleaning_data_upload(conn)                
                json_response = {
                    'status_code' : 200,
                    'description' : "CSV success cleaning and inserted to DB",
                    'data' : data
                }
        else:
                json_response = {
                    'status_code' : 400,
                    'description' : "CSV filed upload"
                }

    else: 
        json_response = {
                'status_code' : 200,
                'description' : "CSV filed upload",
         }
        
    
    response_data = jsonify(json_response)
    return response_data

#End point analisis
@swag_from("yml/analisis.yml", methods=['POST'])
@app.route('/value-analisis', methods=['POST'])
def analisis():
    
    json_response={}
    text = request.form.get('analisis')
   
    
    data =""
    if text=='yes':
        conn = connect_db.connectDB()
        data = cleaning.analisis(conn)
        #convert string to  object
        json_object_data = json.loads(data)
       
        json_response = {
            'status_code' : 200,
            'description' : "Hasil Kalkulasi Cleaning data tweet dari semua data\n",
            'data' : json_object_data
        }
        
    else:
        json_response = {
            'status_code' : 400,
            'description' : "Anda Memilih No. Silahkan Pilih Yes",
            'data' : 'Coba Lagi'
        }
        
    
    response_data = jsonify(json_response)
    return response_data



if __name__ == '__main__':
    #app.run()
    app.run(debug=True)