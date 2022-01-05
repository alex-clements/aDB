from src.Database import Database
from flask import render_template
from flask import Flask, request, jsonify, send_from_directory
import ujson as json
import random
from flask_cors import CORS

app = Flask(__name__, static_url_path='/templates/static/', template_folder="templates")
CORS(app)

database = Database()
# database.index_manager.create_db_index("user_id", "integer")
# database.index_manager.create_db_index("user_name", "string")
# database.index_manager.create_db_index("random_number", "integer")
#
# names = ["Alex", "Julia", "Andrew", "Colin", "Keegan", "Philippe", "Katherine", "name_8"]
#
# for i in range(0, 5000):
#     random_number = random.randint(0, 1000)
#     random_number_2 = random.randint(0, 5000)
#     random_number_name = random.randint(0, len(names)-1)
#     data = {"user_id": random_number,
#             "user_name": names[random_number_name],
#             "bio": "I am the best",
#             "other_details": "kjsd;lksjdf",
#             "birth_date": "05/27/1993",
#             "random_number": random_number_2}
#     database.create_new_item(data)

@app.route("/")
def hello():
    database.create_new_item({"testkey": "what up"})
    return render_template("index.html")

@app.route("/static/css/<path:path>")
def send_css(path):
    return send_from_directory('templates/static/css/', path)

@app.route('/static/js/<path:path>', methods=['GET'])
def send_js(path):
    return send_from_directory('templates/static/js/', path)

@app.route('/static/media/<path:path>', methods=['GET'])
def send_media(path):
    return send_from_directory('templates/static/media/', path)

@app.route('/manifest.json',methods=['GET'])
def send_manifest():
    path = "manifest.json"
    return send_from_directory('templates/', path)

@app.route('/logo192.png',methods=['GET'])
def send_logo():
    path = "logo192.png"
    return send_from_directory('templates/', path)

@app.route('/query', methods=['POST'])
def foo():
    query = request.json
    try:
        new_query = json.loads(query['my_query'])
    except:
        return jsonify(query)

    return_data = database.command_parser(new_query)
    return jsonify(return_data)

if __name__ == "__main__":
    app.run()