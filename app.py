from NetAPorter import NetAPorter

from flask import Flask, request, jsonify, render_template
from flask_cors import CORS

URL = 'https://greendeck-datasets-2.s3.amazonaws.com/netaporter_gb_similar.json'

# Create Flask application
app = Flask(__name__)
CORS(app)

# Importing the class
netaporter = NetAPorter(path = URL)

# Index page
@app.route('/')
def home():
    return 'This is GreenDeck NetAPorter API!!'

@app.route('/getdata', methods=['POST'])
def getData():
    return jsonify(netaporter.readQuery(request.get_json()))

# RUN FLASK APPLICATION
if __name__ == '__main__':
    
    # RUNNNING FLASK APP
    app.run(debug=False)
