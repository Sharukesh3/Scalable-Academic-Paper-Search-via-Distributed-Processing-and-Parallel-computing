from flask import Flask, request, jsonify
from flask_cors import CORS

app = Flask(__name__)
CORS(app) 

@app.route('/search', methods=['GET'])
def search():
    query = request.args.get('q', '')
    if query:
        print(f"Search query received: {query}")
    else:
        print("Empty search query received")
    return jsonify({
        'message': 'Welcome to search engine'
    })

if __name__ == '__main__':
    print("Search engine server running on http://localhost:5000")
    app.run(host='0.0.0.0', port=5000, debug=True)