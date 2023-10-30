from flask import Flask
from flask import jsonify
import oracledb
app = Flask(__name__)

@app.route('/query1', methods=['GET'])
def query1():
    params = oracledb.ConnectParams(host="oracle.cise.ufl.edu", port=1521, service_name="orcl")
    conn = oracledb.connect(user="hvaradarajan", password="Ftaywlr2E6w4dutANw8t0Lth", params=params)
    cursor = conn.cursor()
    cursor.execute('select * from airport')
    while True:
        row = cursor.fetchone()
        if row is None:
            break
        print(row)
    conn.close()
    return jsonify({"message": "Connection successful, Query1"})

@app.route('/query2', methods=['GET'])
def query2():
    params = oracledb.ConnectParams(host="oracle.cise.ufl.edu", port=1521, service_name="orcl")
    conn = oracledb.connect(user="hvaradarajan", password="Ftaywlr2E6w4dutANw8t0Lth", params=params)
    cursor = conn.cursor()
    cursor.execute('select * from borders')
    while True:
        row = cursor.fetchone()
        if row is None:
            break
        print(row)
    conn.close()
    return jsonify({"message": "Connection successful, Query2"})

@app.route('/query3', methods=['GET'])
def query3():
    params = oracledb.ConnectParams(host="oracle.cise.ufl.edu", port=1521, service_name="orcl")
    conn = oracledb.connect(user="hvaradarajan", password="Ftaywlr2E6w4dutANw8t0Lth", params=params)
    cursor = conn.cursor()
    cursor.execute('select * from city')
    while True:
        row = cursor.fetchone()
        if row is None:
            break
        print(row)
    conn.close()
    return jsonify({"message": "Connection successful, Query3"})

@app.route('/query4', methods=['GET'])
def query4():
    params = oracledb.ConnectParams(host="oracle.cise.ufl.edu", port=1521, service_name="orcl")
    conn = oracledb.connect(user="hvaradarajan", password="Ftaywlr2E6w4dutANw8t0Lth", params=params)
    cursor = conn.cursor()
    cursor.execute('select * from country')
    while True:
        row = cursor.fetchone()
        if row is None:
            break
        print(row)
    conn.close()
    return jsonify({"message": "Connection successful, Query4"})

@app.route('/query5', methods=['GET'])
def query5():
    params = oracledb.ConnectParams(host="oracle.cise.ufl.edu", port=1521, service_name="orcl")
    conn = oracledb.connect(user="hvaradarajan", password="Ftaywlr2E6w4dutANw8t0Lth", params=params)
    cursor = conn.cursor()
    cursor.execute('select * from desert')
    while True:
        row = cursor.fetchone()
        if row is None:
            break
        print(row)
    conn.close()
    return jsonify({"message": "Connection successful, Query5"})

if __name__ == '__main__':
    app.run(host='localhost', port=5000)