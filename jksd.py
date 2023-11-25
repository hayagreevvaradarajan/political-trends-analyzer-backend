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
                            WHERE START_YEAR BETWEEN {start_date} AND {end_date}
                            UNION ALL
                            SELECT prez_name, prez_start_date + 1, prez_end_date
                            FROM prez_data
                            WHERE prez_data.prez_start_date + 1 <= {end_date}
                              AND prez_data.prez_start_date + 1 < prez_data.prez_end_date
                        )
                        SELECT *
                        FROM prez_data
                    )
                    ORDER BY prez_start_date
                ) b
                ON a.year = b.prez_start_date
                WHERE productorsector = 'Pharmaceuticals'*sector
                ORDER BY prez_start_date
            ) a
            JOIN USGDPDATA b
            ON a.prez_start_date = b.year
        )
    )
    WHERE row_num = 1
);
"""