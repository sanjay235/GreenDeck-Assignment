from flask import Flask, request, jsonify, render_template
from NetAPorter import NetAPorter
from flask_cors import CORS
import urllib.request
import flask
import os
import gc

url = 'https://greendeck-datasets-2.s3.amazonaws.com/netaporter_gb_similar.json'

# Importing the class
netaporter = NetAPorter(path = 'dumps/netaporter_gb.json')

# Create Flask application
app = flask.Flask(__name__)
CORS(app)

# Index page
@app.route('/', methods=['GET', 'POST'])
def home():
    result = jsonify(netaporter.readQuery(request.get_json()))
    print('Garbage Collector:', gc.collect())
    return result

def init_files(dump_path = 'dumps/netaporter_gb.json'):
    
    if dump_path.split('/')[0] not in os.listdir():
        os.mkdir(dump_path.split('/')[0])
    
    if not os.path.exists(dump_path):
        urllib.request.urlretrieve(url, dump_path)

# RUN FLASK APPLICATION
if __name__ == '__main__':
    
    # GETTING DATASET this function will download the dataset
    init_files('dumps/netaporter_gb.json') 
    
    # RUNNNING FLASK APP
    app.run(debug=True) 
