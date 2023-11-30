SELECT
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
WHERE statename = '{state_name}' and year >= {start_date} and year <= {end_date}  -- Replace 'CALIFORNIA' with the desired state name
GROUP BY
    year,
    statename
ORDER BY
    year,
    statename;