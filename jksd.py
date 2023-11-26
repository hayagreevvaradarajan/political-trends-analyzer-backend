sql_query = f"""select* from(select * from (
(
select statename,year,round((gdp/population),2)state_gdp_per_capita from(
(select STATENAME,YEAR,GDP from USSTATEGDPDATA where quarter='Q4' order by year) a
natural join(select * from USSTATEPOPULATIONDATA)
))  
natural join(
select year, round((gdp/us_population),2)US_GDP_PER_CAPITA 
from(
(select * from USGDPDATA) 
natural join
(select year,sum(population)US_POPULATION from USSTATEPOPULATIONDATA group by(year)order by year))))"""