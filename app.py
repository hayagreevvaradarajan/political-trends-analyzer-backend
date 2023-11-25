from flask import Flask
from flask import jsonify
import oracledb

app = Flask(__name__)


@app.route('/query1', methods=['GET'])
def query1():
    params = oracledb.ConnectParams(host="oracle.cise.ufl.edu", port=1521, service_name="orcl")
    conn = oracledb.connect(user="v.vadlamani", password="XEfjppuxN8M49BF8ccGDvnPf", params=params)
    cursor = conn.cursor()
    sql_query = sql_query = """
    SELECT prez_name, prez_start_date, ROUND(((total_sum * 1000000) / gdp) * 100, 4) AS gdp_contri
    FROM (
        SELECT prez_name, gdp, prez_start_date, total_sum, row_num
        FROM (
            SELECT prez_name, gdp, prez_start_date, total_sum, row_num
            FROM (
                SELECT prez_name, gdp, prez_start_date, 
                       SUM(tradevaluem) OVER (PARTITION BY prez_start_date ORDER BY prez_start_date) AS total_sum,
                    ROW_NUMBER() OVER (PARTITION BY prez_start_date ORDER BY prez_start_date DESC) AS row_num
                FROM (
                SELECT prez_name, productorsector, tradevaluem, prez_start_date
                FROM USTRADEDATA a
                JOIN (
                    SELECT *
                    FROM (
                        WITH prez_data(prez_name, prez_start_date, prez_end_date) AS (
                            SELECT name, START_YEAR, END_YEAR
                            FROM (
                                SELECT PRESIDENT.name,
                                       EXTRACT(YEAR FROM PRESIDENT.start_date) AS START_YEAR,
                                       EXTRACT(YEAR FROM PRESIDENT.end_date) AS END_YEAR
                                FROM PRESIDENT
                            ) 
                            WHERE START_YEAR BETWEEN 2000 AND 2020
                            UNION ALL
                            SELECT prez_name, prez_start_date + 1, prez_end_date
                            FROM prez_data
                            WHERE prez_data.prez_start_date + 1 <= 2020 
                              AND prez_data.prez_start_date + 1 < prez_data.prez_end_date
                        )
                        SELECT *
                        FROM prez_data
                    )
                    ORDER BY prez_start_date
                    ) b
                    ON a.year = b.prez_start_date
                    WHERE productorsector = 'Pharmaceuticals'
                    ORDER BY prez_start_date
                ) a
                JOIN USGDPDATA b
                ON a.prez_start_date = b.year
            )
        )
        WHERE row_num = 1
    )
    """
    cursor.execute(sql_query)
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
    conn = oracledb.connect(user="v.vadlamani", password="XEfjppuxN8M49BF8ccGDvnPf", params=params)
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
    app.run(host='localhost', port=3000)
