sql_query = f"""WITH RankedVotes AS (
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
    AND F.statename = '{state_name}'
    AND R.year BETWEEN '{start_date}' AND '{end_date}'"""