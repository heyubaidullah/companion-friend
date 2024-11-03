from flask import Flask, request, jsonify
from pyspark.sql import SparkSession

app = Flask(__name__)

# Initialize Spark session
spark = SparkSession.builder.appName("ArcadeTherapistAPI").getOrCreate()

def authenticate_user(username, password):
    user_data = spark.sql(f"SELECT * FROM arcade_therapist.user_auth WHERE username = '{username}'")
    if user_data.count() == 0:
        return False, "User not found."
    stored_password = user_data.first().password
    if password == stored_password:
        return True, "Login successful."
    else:
        return False, "Invalid password."

@app.route('/')
def home():
    return "Welcome to Arcade Therapist API"

@app.route('/login', methods=['POST'])
def login():
    data = request.json
    username = data.get('username')
    password = data.get('password')

    success, message = authenticate_user(username, password)
    return jsonify({"message": message})

if __name__ == "__main__":
    app.run(host='0.0.0.0', port=5000)