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