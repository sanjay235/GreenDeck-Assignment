from flask import Flask, request, jsonify, render_template
from NetAPorter import NetAPorter
from flask_cors import CORS
import flask
import os
import gc

url = 'https://greendeck-datasets-2.s3.amazonaws.com/netaporter_gb_similar.json'

# Importing the class
netaporter = NetAPorter(path = url)

# Create Flask application
app = flask.Flask(__name__)
CORS(app)

# Index page
@app.route('/', methods=['POST'])
def home():
    result = jsonify(netaporter.readQuery(request.get_json()))
    print('Garbage Collector:', gc.collect())
    return result

# RUN FLASK APPLICATION
if __name__ == '__main__':
    
    # RUNNNING FLASK APP
    app.run(debug=True) 
