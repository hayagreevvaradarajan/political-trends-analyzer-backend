from flask import Flask
from flask import jsonify
import pyodbc
import oracledb
app = Flask(__name__)

@app.route('/', methods=['GET', 'POST'])
def welcome():
    params = oracledb.ConnectParams(host="oracle.cise.ufl.edu", port=1521, service_name="orcl")
    conn = oracledb.connect(user="hvaradarajan", password="Ftaywlr2E6w4dutANw8t0Lth", params=params)
    cursor = conn.cursor()
    cursor.execute('select * from airport')
    while True:
        row = cursor.fetchone()
        if row is None:
            break
        print(row)
    return jsonify({"message": "Connection successful"})
if __name__ == '__main__':
    app.run(host='localhost', port=5000)