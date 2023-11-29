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
    data_array = []
    cursor.execute(sql_query)
    description = [description[0] for description in cursor.description]
    while True:
        try:
            row = list(cursor.fetchone())
            data_dict = {}
            if row is not None:
                for i in range(len(description)):
                    record = {
                        description[i]:row[i]
                    }
                    data_dict.update(record)
                    print(data_dict)
                data_array.append(data_dict)
        except Exception as e:
            print(e)
            break
    conn.close()
    output_dict = {
        "data": data_array
    }
    return jsonify(output_dict)

@app.route('/total_count', methods=['GET'])
def total_count():
    params = oracledb.ConnectParams(host="oracle.cise.ufl.edu", port=1521, service_name="orcl")
    conn = oracledb.connect(user="v.vadlamani", password="XEfjppuxN8M49BF8ccGDvnPf", params=params)
    cursor = conn.cursor()
    sql_query = """SELECT SUM(total_rows) AS total_rows_across_tables
FROM (
    SELECT COUNT(*) AS total_rows FROM USSTATEPOPULATIONDATA
    UNION ALL
    SELECT COUNT(*) FROM USSTATEPRESIDENTDATA
    UNION ALL
    SELECT COUNT(*) FROM USTRADEDATA
    UNION ALL
    SELECT COUNT(*) FROM USCOUNTYFIPSCODE
    UNION ALL
    SELECT COUNT(*) FROM USAHOUSEAGETABLE
    UNION ALL
    SELECT COUNT(*) FROM USSTATEGDPDATA
    UNION ALL
    SELECT COUNT(*) FROM USGDPDATA
    UNION ALL
    SELECT COUNT(*) FROM USCOUNTYPOPULATIONDATA
    UNION ALL
    SELECT COUNT(*) FROM SENATEVOTES
    UNION ALL
    SELECT COUNT(*) FROM USSTATEFIPSCODE
    UNION ALL
    SELECT COUNT(*) FROM HORPOPULARVOTE
    UNION ALL
    SELECT COUNT(*) FROM USSTATEGUNDEATHS
    UNION ALL
    SELECT COUNT(*) FROM USYEARUNEMPLOYEMENT
    UNION ALL
    SELECT COUNT(*) FROM UNEMPLOYEMENTDATA
) all_tables"""
    data_array = []
    cursor.execute(sql_query)
    description = [description[0] for description in cursor.description]
    while True:
        try:
            row = list(cursor.fetchone())
            data_dict = {}
            if row is not None:
                for i in range(len(description)):
                    record = {
                        description[i]:row[i]
                    }
                    data_dict.update(record)
                    print(data_dict)
                data_array.append(data_dict)
        except Exception as e:
            print(e)
            break
    conn.close()
    output_dict = {
        "data": data_array
    }
    return jsonify(output_dict)

@app.route('/query2', methods=['GET'])
def query2():
    params = oracledb.ConnectParams(host="oracle.cise.ufl.edu", port=1521, service_name="orcl")
    conn = oracledb.connect(user="v.vadlamani", password="XEfjppuxN8M49BF8ccGDvnPf", params=params)
    cursor = conn.cursor()
    start_date = request.args.get('start_date')
    end_date = request.args.get('end_date')
    state_name = request.args.get('state_name')
    sql_query1 = f"""SELECT
    year,
    statename,
    ROUND((SUM(average_row_age * people_number) / NULLIF(SUM(people_number), 0)), 2) as avg_age
FROM
    (
        SELECT
            uscountypopulationdata.year,
            uscountyfipscode.countyname,
            usstatefipscode.statename,
            ROUND(
                (
                    (
                        uscountypopulationdata.agecategoryone * 2 +
                        uscountypopulationdata.agecategorytwo * 7 +
                        uscountypopulationdata.agecategorythree * 12 +
                        uscountypopulationdata.agecategoryfour * 17 +
                        uscountypopulationdata.agecategoryfive * 22 +
                        uscountypopulationdata.agecategorysix * 27 +
                        uscountypopulationdata.agecategoryseven * 32 +
                        uscountypopulationdata.agecategoryeight * 37 +
                        uscountypopulationdata.agecategorynine * 42 +
                        uscountypopulationdata.agecategoryten * 47 +
                        uscountypopulationdata.agecategoryeleven * 52 +
                        uscountypopulationdata.agecategorytwelve * 57 +
                        uscountypopulationdata.agecategorythirteen * 62 +
                        uscountypopulationdata.agecategoryfourteen * 67 +
                        uscountypopulationdata.agecategoryfifteen * 72 +
                        uscountypopulationdata.agecategorysixteen * 77 +
                        uscountypopulationdata.agecategoryseventeen * 82 +
                        uscountypopulationdata.agecategoryeighteen * 87
                    ) / ((
                        uscountypopulationdata.agecategoryone +
                        uscountypopulationdata.agecategorytwo +
                        uscountypopulationdata.agecategorythree +
                        uscountypopulationdata.agecategoryfour +
                        uscountypopulationdata.agecategoryfive +
                        uscountypopulationdata.agecategorysix +
                        uscountypopulationdata.agecategoryseven +
                        uscountypopulationdata.agecategoryeight +
                        uscountypopulationdata.agecategorynine +
                        uscountypopulationdata.agecategoryten +
                        uscountypopulationdata.agecategoryeleven +
                        uscountypopulationdata.agecategorytwelve +
                        uscountypopulationdata.agecategorythirteen +
                        uscountypopulationdata.agecategoryfourteen +
                        uscountypopulationdata.agecategoryfifteen +
                        uscountypopulationdata.agecategorysixteen +
                        uscountypopulationdata.agecategoryseventeen +
                        uscountypopulationdata.agecategoryeighteen
                    ) + 1)
                ), 2
            ) AS average_row_age,
            (
                uscountypopulationdata.agecategoryone +
                uscountypopulationdata.agecategorytwo +
                uscountypopulationdata.agecategorythree +
                uscountypopulationdata.agecategoryfour +
                uscountypopulationdata.agecategoryfive +
                uscountypopulationdata.agecategorysix +
                uscountypopulationdata.agecategoryseven +
                uscountypopulationdata.agecategoryeight +
                uscountypopulationdata.agecategorynine +
                uscountypopulationdata.agecategoryten +
                uscountypopulationdata.agecategoryeleven +
                uscountypopulationdata.agecategorytwelve +
                uscountypopulationdata.agecategorythirteen +
                uscountypopulationdata.agecategoryfourteen +
                uscountypopulationdata.agecategoryfifteen +
                uscountypopulationdata.agecategorysixteen +
                uscountypopulationdata.agecategoryseventeen +
                uscountypopulationdata.agecategoryeighteen
            ) AS people_number
        FROM
            uscountypopulationdata
        JOIN
            uscountyfipscode ON uscountypopulationdata.countycode = uscountyfipscode.fipscode
        JOIN
            usstatefipscode ON uscountyfipscode.statefipscode = usstatefipscode.fipscode
    ) county_average_age
    where statename = '{state_name.upper()}'
GROUP BY
    year,
    statename
ORDER BY
    year,
    statename"""
    
    sql_query2 = f"""SELECT
    statename,
    termstartdate,
    ROUND(AVG(age), 2) as average_age
FROM
    usahouseagetable
WHERE
    termstartdate > TO_DATE('1965-01-01', 'YYYY-MM-DD') AND
    termstartdate < TO_DATE('1985-01-01', 'YYYY-MM-DD') AND
    UPPER(statename) = '{state_name.upper()}'
GROUP BY
    statename,
    termstartdate
ORDER BY
    termstartdate,
    statename"""
    
    data_array_1 = []
    cursor.execute(sql_query1)
    description = [description[0] for description in cursor.description]
    while True:
        try:
            row = list(cursor.fetchone())
            data_dict = {}
            if row is not None:
                for i in range(len(description)):
                    record = {
                        description[i]:row[i]
                    }
                    data_dict.update(record)
                    print(data_dict)
                data_array_1.append(data_dict)
        except Exception as e:
            print(e)
            break
    data_array_2 = []
    cursor.execute(sql_query2)
    description2 = [description[0] for description in cursor.description]
    while True:
        try:
            row = list(cursor.fetchone())
            data_dict = {}
            if row is not None:
                for i in range(len(description2)):
                    record = {
                        description2[i]:row[i]
                    }
                    data_dict.update(record)
                    print(data_dict)
                data_array_2.append(data_dict)
        except Exception as e:
            print(e)
            break
    conn.close()
    output_dict = {
        "data_graph1": data_array_1,
        "data_graph2": data_array_2
    }
    return jsonify(output_dict)

@app.route('/query3', methods=['GET'])
def query3():
    params = oracledb.ConnectParams(host="oracle.cise.ufl.edu", port=1521, service_name="orcl")
    conn = oracledb.connect(user="v.vadlamani", password="XEfjppuxN8M49BF8ccGDvnPf", params=params)
    cursor = conn.cursor()
    start_date = request.args.get('start_date')
    end_date = request.args.get('end_date')
    state_name = request.args.get('state_name')
    sql_query1 = f"""SELECT
    p.statename,
    p.year,
    g.deathcount,
    p.population,
    (g.deathcount / p.population) * 100000 AS deaths_per_100000
FROM
    USSTATEPOPULATIONDATA p
JOIN
    USSTATEGUNDEATHS g ON p.statename = g.statename AND p.year = g.year
WHERE
    p.statename = '{state_name}'  
    AND p.year BETWEEN '{start_date}' AND '{end_date}'"""
    
    sql_query2 = f"""WITH RankedVotes AS (
    SELECT
        id,
        statefipscode,
        partyname,
        year,
        candidatename,
        candidatevotes,
        totalvotes,
        ROW_NUMBER() OVER (PARTITION BY year, statefipscode ORDER BY candidatevotes DESC) AS Rank
    FROM
        SENATEVOTES
)
SELECT
    R.id,
    R.statefipscode,
    F.statename AS state_name,
    R.year,
    R.candidatename AS ruling_candidate,
    R.partyname AS ruling_party,
    (R.candidatevotes * 100.0 / R.totalvotes) AS vote_percentage
FROM
    RankedVotes R
JOIN
    USSTATEFIPSCODE F ON R.statefipscode = F.fipscode
WHERE
    R.Rank = 1
    AND F.statename = '{state_name.upper()}'
    AND R.year BETWEEN '{start_date}' AND '{end_date}'"""
    data_array_1 = []
    cursor.execute(sql_query1)
    description = [description[0] for description in cursor.description]
    while True:
        try:
            row = list(cursor.fetchone())
            data_dict = {}
            if row is not None:
                for i in range(len(description)):
                    record = {
                        description[i]:row[i]
                    }
                    data_dict.update(record)
                    print(data_dict)
                data_array_1.append(data_dict)
        except Exception as e:
            print(e)
            break
    data_array_2 = []
    cursor.execute(sql_query2)
    description2 = [description[0] for description in cursor.description]
    while True:
        try:
            row = list(cursor.fetchone())
            data_dict = {}
            if row is not None:
                for i in range(len(description2)):
                    record = {
                        description2[i]:row[i]
                    }
                    data_dict.update(record)
                    print(data_dict)
                data_array_2.append(data_dict)
        except Exception as e:
            print(e)
            break
    conn.close()
    output_dict = {
        "data_graph1": data_array_1,
        "data_graph2": data_array_2
    }
    return jsonify(output_dict)

@app.route('/query4', methods=['GET'])
def query4():
    params = oracledb.ConnectParams(host="oracle.cise.ufl.edu", port=1521, service_name="orcl")
    conn = oracledb.connect(user="v.vadlamani", password="XEfjppuxN8M49BF8ccGDvnPf", params=params)
    cursor = conn.cursor()
    start_date = request.args.get('start_date')
    end_date = request.args.get('end_date')
    state_name = request.args.get('state_name')
    sql_query1 = f"""
SELECT
    usstatepresidentdata.YEAR,
    usstatepresidentdata.STATENAME,
    CANDIDATENAME,
    CANDIDATEVOTES,
    TOTALVOTES,
    PARTY,
    usstatepopulationdata.population as total_population,
    ROUND((TOTALVOTES/USSTATEPOPULATIONDATA.POPULATION) * 100, 2) AS VOTER_PARTICIPATION_PERCENTAGE,
    ROUND((CANDIDATEVOTES / TOTALVOTES) * 100, 2) AS POPULAR_VOTE_PERCENTAGE,
    CASE
        WHEN RANK() OVER (PARTITION BY usstatepresidentdata.YEAR, usstatepresidentdata.STATENAME ORDER BY CANDIDATEVOTES DESC) = 1 THEN 'Y'
        ELSE 'N'
    END AS Winner
FROM
    USSTATEPRESIDENTDATA, usstatepopulationdata
WHERE
    usstatepresidentdata.STATENAME = '{state_name.upper()}' and --Florida
    upper(usstatepresidentdata.statename) = upper(usstatepopulationdata.statename)
    and usstatepresidentdata.year = usstatepopulationdata.year
    AND usstatepresidentdata.YEAR BETWEEN {start_date} AND {end_date} --1978 and 2010
ORDER BY
    usstatepresidentdata.YEAR
"""
    sql_query2 = f"""SELECT
        YEAR,
        INDICATOR,
        ROUND(SUM(TRADEVALUEM), 2) AS TOTAL_VALUE
    FROM
        USTRADEDATA
    WHERE
        YEAR BETWEEN '{start_date}' AND '{end_date}'
    GROUP BY
        YEAR, INDICATOR
    ORDER BY
        YEAR"""
    data_array_1 = []
    cursor.execute(sql_query1)
    description = [description[0] for description in cursor.description]
    while True:
        try:
            row = list(cursor.fetchone())
            data_dict = {}
            if row is not None:
                for i in range(len(description)):
                    record = {
                        description[i]:row[i]
                    }
                    data_dict.update(record)
                    print(data_dict)
                data_array_1.append(data_dict)
        except Exception as e:
            print(e)
            break
    data_array_2 = []
    cursor.execute(sql_query2)
    description2 = [description[0] for description in cursor.description]
    while True:
        try:
            row = list(cursor.fetchone())
            data_dict = {}
            if row is not None:
                for i in range(len(description2)):
                    record = {
                        description2[i]:row[i]
                    }
                    data_dict.update(record)
                    print(data_dict)
                data_array_2.append(data_dict)
        except Exception as e:
            print(e)
            break
    conn.close()
    output_dict = {
        "data_graph1": data_array_1,
        "data_graph2": data_array_2
    }
    return jsonify(output_dict)

@app.route('/query5', methods=['GET'])
def query5():
    params = oracledb.ConnectParams(host="oracle.cise.ufl.edu", port=1521, service_name="orcl")
    conn = oracledb.connect(user="v.vadlamani", password="XEfjppuxN8M49BF8ccGDvnPf", params=params)
    cursor = conn.cursor()
    state_name = request.args.get('state_name')
    start_date = request.args.get('start_date')
    end_date = request.args.get('end_date')
    sql_query1 = f"""SELECT
    S.statefipscode,
    F.statename,
    S.year,
    ROUND(SUM(CASE WHEN S.partyname = 'REPUBLICAN' THEN S.candidatevotes ELSE 0 END) / SUM(DISTINCT S.totalvotes) * 100,2) AS republican_vote_percentage,
    ROUND(SUM(CASE WHEN S.partyname = 'DEMOCRAT' THEN S.candidatevotes ELSE 0 END) / SUM(DISTINCT S.totalvotes) * 100,2) AS democrat_vote_percentage,
    SUM(DISTINCT S.totalvotes) AS total_votes
FROM
    HORPOPULARVOTE S
JOIN
    USSTATEFIPSCODE F ON S.statefipscode = F.fipscode
WHERE
    F.statename = '{state_name.upper()}'  -- Replace 'YourState' with the desired state name
    AND S.year BETWEEN '2005' AND '2019'  -- Replace 'StartDate' and 'EndDate' with the desired date range
GROUP BY
    S.statefipscode,
    F.statename,
    S.year
ORDER BY
    S.year"""
    sql_query2 = f"""select * from(select * from (
(
select statename,year,round((gdp/population),2)state_gdp_per_capita from(
(select STATENAME,YEAR,GDP  from USSTATEGDPDATA where quarter='Q4' and statename='{state_name}' order by year) a
natural join(select * from USSTATEPOPULATIONDATA)
))  
natural join(
select year ,round((gdp/us_population),2)US_GDP_PER_CAPITA 
from(
(select * from USGDPDATA) 
natural join
(select year,sum(population)US_POPULATION from USSTATEPOPULATIONDATA group by(year)order by year)))))"""
    data_array_1 = []
    cursor.execute(sql_query1)
    description = [description[0] for description in cursor.description]
    while True:
        try:
            row = list(cursor.fetchone())
            data_dict = {}
            if row is not None:
                for i in range(len(description)):
                    record = {
                        description[i]:row[i]
                    }
                    data_dict.update(record)
                    print(data_dict)
                data_array_1.append(data_dict)
        except Exception as e:
            print(e)
            break
    data_array_2 = []
    cursor.execute(sql_query2)
    description2 = [description[0] for description in cursor.description]
    while True:
        try:
            row = list(cursor.fetchone())
            data_dict = {}
            if row is not None:
                for i in range(len(description2)):
                    record = {
                        description2[i]:row[i]
                    }
                    data_dict.update(record)
                    print(data_dict)
                data_array_2.append(data_dict)
        except Exception as e:
            print(e)
            break     
    conn.close()
    output_dict = {
        "data_graph1": data_array_1,
        "data_graph2": data_array_2
    }
    return jsonify(output_dict)

@app.route('/query6', methods=['GET'])
def query6():
    params = oracledb.ConnectParams(host="oracle.cise.ufl.edu", port=1521, service_name="orcl")
    conn = oracledb.connect(user="v.vadlamani", password="XEfjppuxN8M49BF8ccGDvnPf", params=params)
    cursor = conn.cursor()
    state_name = request.args.get('state_name')
    start_date = request.args.get('start_date')
    end_date = request.args.get('end_date')
    sql_query1 = f"""
SELECT
    U.year,
    U.state,
    ROUND(AVG(U.rate), 2) AS state_avg_unemployment_rate,
    
    ROUND(AVG(Y.rate), 2) AS country_avg_unemployment_rate_yeartodate,
    P.name AS president_name,
    P.party AS president_party,
    P.start_date AS president_start_date,
    P.end_date AS president_end_date
FROM
    UNEMPLOYEMENTDATA U
JOIN
    USSTATEFIPSCODE F ON UPPER(U.state) = UPPER(F.statename)
JOIN
    PRESIDENT P ON U.year BETWEEN EXTRACT(YEAR FROM P.start_date) AND EXTRACT(YEAR FROM P.end_date)
LEFT JOIN
    USYEARUNEMPLOYEMENT Y ON U.year = Y.year
WHERE
    UPPER(U.state) = '{state_name.upper()}' --Alabama
    AND U.year BETWEEN '{start_date}' AND '{end_date}' --2000 and 2005
GROUP BY
    U.year,
    U.state,
    P.name,
    P.party,
    P.start_date,
    P.end_date
ORDER BY
    U.year,
    U.state
"""
    sql_query2 = f"""
SELECT
    S.statefipscode,
    F.statename,
    S.year,
    ROUND(SUM(CASE WHEN S.partyname = 'REPUBLICAN' THEN S.candidatevotes ELSE 0 END) / SUM(DISTINCT S.totalvotes) * 100,2) AS republican_vote_percentage,
    ROUND(SUM(CASE WHEN S.partyname = 'DEMOCRAT' THEN S.candidatevotes ELSE 0 END) / SUM(DISTINCT S.totalvotes) * 100,2) AS democrat_vote_percentage,
    SUM(DISTINCT S.totalvotes) AS total_votes
FROM
    HORPOPULARVOTE S
JOIN
    USSTATEFIPSCODE F ON S.statefipscode = F.fipscode
WHERE
    F.statename = '{state_name.upper()}'  -- CALIFORNIA
    AND S.year BETWEEN '{start_date}' AND '{end_date}'  -- 2005 and 2019
GROUP BY
    S.statefipscode,
    F.statename,
    S.year
ORDER BY
    S.year
"""
    data_array_1 = []
    cursor.execute(sql_query1)
    description = [description[0] for description in cursor.description]
    while True:
        try:
            row = list(cursor.fetchone())
            data_dict = {}
            if row is not None:
                for i in range(len(description)):
                    record = {
                        description[i]:row[i]
                    }
                    data_dict.update(record)
                    print(data_dict)
                data_array_1.append(data_dict)
        except Exception as e:
            print(e)
            break
    data_array_2 = []
    cursor.execute(sql_query2)
    description2 = [description[0] for description in cursor.description]
    while True:
        try:
            row = list(cursor.fetchone())
            data_dict = {}
            if row is not None:
                for i in range(len(description2)):
                    record = {
                        description2[i]:row[i]
                    }
                    data_dict.update(record)
                    print(data_dict)
                data_array_2.append(data_dict)
        except Exception as e:
            print(e)
            break     
    conn.close()
    output_dict = {
        "data_graph1": data_array_1,
        "data_graph2": data_array_2
    }
    return jsonify(output_dict)

if __name__ == '__main__':
    app.run(host='localhost', port=3000)
