from flask import Flask, request
from flask import jsonify
import oracledb
from flask_cors import CORS
app = Flask(__name__)
CORS(app)

@app.route('/query1', methods=['GET'])
def query1():
    params = oracledb.ConnectParams(host="oracle.cise.ufl.edu", port=1521, service_name="orcl")
    conn = oracledb.connect(user="v.vadlamani", password="XEfjppuxN8M49BF8ccGDvnPf", params=params)
    cursor = conn.cursor()
    start_date = request.args.get('start_date')
    end_date = request.args.get('end_date')
    sector = request.args.get('sector')
    sql_query = f"""
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
                            WHERE START_YEAR BETWEEN '{start_date}' AND '{end_date}'
                            UNION ALL
                            SELECT prez_name, prez_start_date + 1, prez_end_date
                            FROM prez_data
                            WHERE prez_data.prez_start_date + 1 <= '{end_date}'
                              AND prez_data.prez_start_date + 1 < prez_data.prez_end_date
                        )
                        SELECT *
                        FROM prez_data
                    )
                    ORDER BY prez_start_date
                ) b
                ON a.year = b.prez_start_date
                WHERE productorsector = '{sector}'
                ORDER BY prez_start_date
            ) a
            JOIN USGDPDATA b
            ON a.prez_start_date = b.year
        )
    )
    WHERE row_num = 1
)
"""
    date_range = []
    data_array = []
    for i in range(int(start_date), int(end_date)+1):
        date_range.append(i)
    cursor.execute(sql_query)
    description = [description[0] for description in cursor.description]
    while True:
        try:
            row = list(cursor.fetchone())
            data_dict = {}
            if row is None:
                break
            for i in range(len(description)):
                record = {
                    description[i]:row[i]
                }
                data_dict.update(record)
                print(data_dict)
        except Exception as e:
            print(e)
            break
        finally:
            data_array.append(data_dict)
    conn.close()
    output_dict = {
        "date_range": date_range,
        "data_array": data_array
    }
    return jsonify(output_dict)
@app.route('/query2', methods=['GET'])
def query2():
    params = oracledb.ConnectParams(host="oracle.cise.ufl.edu", port=1521, service_name="orcl")
    conn = oracledb.connect(user="v.vadlamani", password="XEfjppuxN8M49BF8ccGDvnPf", params=params)
    cursor = conn.cursor()
    request_param = request.args
    cursor.execute('select * from borders')
    while True:
        row = cursor.fetchone()
        if row is None:
            break
        print(row)
    conn.close()
    # return jsonify({"message": "Connection successful, Query2"})
    return request_param
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
    data_array = []
    cursor.execute('select * from country')
    description = [description[0] for description in cursor.description]
    while True:
        try:
            row = list(cursor.fetchone())
            data_dict = {}
            if row is None:
                break
            for i in range(len(description)):
                record = {
                    description[i]:row[i]
                }
                data_dict.update(record)
        except:
            break
        finally:
            data_array.append(data_dict)
    conn.close()
    # print(description)
    # return jsonify({"message": "Connection successful, Query4"})
    return data_array

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
