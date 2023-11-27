sql_query1 = f"""
WITH RankedVotes AS (
    SELECT
        H.id,
        H.statefipscode,
        F.statename,
        H.year,
        H.candidatename,
        H.partyname,
        H.candidatevotes,
        H.totalvotes,
        RANK() OVER (PARTITION BY H.statefipscode, H.year ORDER BY
            CASE WHEN H.totalvotes > 0 AND H.candidatevotes IS NOT NULL AND H.totalvotes IS NOT NULL
                 THEN (H.candidatevotes * 100.0 / H.totalvotes)
                 ELSE 0
            END DESC) AS VoteRank
    FROM
        HORPOPULARVOTE H
    JOIN
        USSTATEFIPSCODE F ON H.statefipscode = F.fipscode
    WHERE
        F.statename = '{state_name.upper()}' 
        AND H.year BETWEEN '{start_date}' AND '{end_date}'
)
SELECT
    id,
    statefipscode,
    statename,
    year,
    candidatename,
    partyname,
    candidatevotes,
    totalvotes,
    CASE WHEN totalvotes > 0 AND candidatevotes IS NOT NULL AND totalvotes IS NOT NULL
         THEN (candidatevotes * 100.0 / totalvotes)
         ELSE 0
    END AS vote_percentage
FROM
    RankedVotes
WHERE
    VoteRank <= 2
"""

sql_query2 = f"""
select* from(select * from (
(
select statename,year,round((gdp/population),2)state_gdp_per_capita from(
(select STATENAME,YEAR,GDP  from USSTATEGDPDATA where quarter='Q4' order by year) a
natural join(select * from USSTATEPOPULATIONDATA)
))  
natural join(
select year,round((gdp/us_population),2) US_GDP_PER_CAPITA 
from(
(select * from  USGDPDATA) 
natural join
(select year,sum(population)US_POPULATION from USSTATEPOPULATIONDATA group by(year)order by year))))
"""