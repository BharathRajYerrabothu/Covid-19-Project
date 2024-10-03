--COVID 19 DATA EXPLORATION
ALTER DATABASE ProtfolioProject MODIFY NAME = PortfolioProject
SELECT *
FROM PortfolioProject ..coviddeaths
WHERE continent!=''
ORDER BY 3,4

-- Choose the initial dataset we'll be working with

SELECT location,continent, DATE, population, total_cases, total_deaths, new_cases
FROM PortfolioProject ..coviddeaths
WHERE continent!=''
AND total_cases > 0
ORDER BY 1,2

--Total Cases vs Total Deaths 
-- Reflects the risk of death if you contract COVID-19 in your country

SELECT location, DATE, total_cases, total_deaths, (total_deaths/total_cases) * 100 AS deathpercentage
FROM PortfolioProject ..coviddeaths
WHERE location like '%INDIA%' 
and continent!=''
AND total_cases > 0
ORDER BY deathpercentage DESC

-- Top 10 locations with the highest case fatality rate

SELECT TOP 10
    location,
    (SUM(total_deaths) * 1.0 / NULLIF(SUM(total_cases), 0)) * 100 AS case_fatality_rate
FROM PortfolioProject..coviddeaths
WHERE continent != ''
GROUP BY location
ORDER BY case_fatality_rate DESC;

-- Total cases and deaths by continent and Percentage of Deaths

SELECT location,
       SUM(total_cases) AS total_cases,
       SUM(total_deaths) AS total_deaths, SUM(total_deaths)/SUM(total_cases) * 100 AS Percent_Of_Deaths
FROM PortfolioProject..coviddeaths
WHERE continent is null
and location not in ('World', 'High-income countries','Upper-middle-income countries', 'European Union (27)', 'Lower-middle-income countries', 'Low-income countries'  )
GROUP BY location
ORDER BY total_cases DESC;


--Total Death count

Select location, SUM(cast(new_deaths as int)) as TotalDeathCount
From PortfolioProject..CovidDeaths
Where continent is null 
and location not in ('World', 'High-income countries','Upper-middle-income countries', 'European Union (27)', 'Lower-middle-income countries', 'Low-income countries'  )
GROUP BY location
order by TotalDeathCount desc


--Total Cases vs Population
--Displays the percentage of the population infected with COVID-19

SELECT DISTINCT location, continent, total_cases, population, (total_cases/population)*100 AS InfectionRatepercent
FROM PortfolioProject ..coviddeaths
WHERE continent!=''
ORDER BY InfectionRatepercent desc

--Countries with the Highest COVID-19 Infection Rates Relative to Population

SELECT location, population, max(total_cases) as MaximumInfectionCount, max(total_cases/population)*100 AS PercentofPopulationInfected
FROM PortfolioProject ..coviddeaths
WHERE continent!=''
GROUP BY location, population
ORDER BY PercentofPopulationInfected DESC


--Countries with the Total Death Count

SELECT location, max(total_deaths) as Totaldeathcount
FROM PortfolioProject ..coviddeaths
WHERE continent!=''
GROUP BY location
ORDER BY Totaldeathcount DESC

--Highlighting Continents with the Highest Deaths per Capita

SELECT continent, sum(new_deaths) AS Totaldeathcount
FROM PortfolioProject ..coviddeaths
where continent!=''
group by continent
ORDER BY Totaldeathcount DESC

--Global Numbers

SELECT sum(new_deaths) AS Total_death, sum(new_cases) as Total_cases, (sum(new_deaths)/sum(new_cases))*100 as Deathpercentage 
FROM PortfolioProject ..coviddeaths
where continent!='';

--TOTAL POPULATION VS TOTAL VACCINATIONS
--Displays the percentage of the population that has received at least one COVID vaccine

SELECT dea.continent, dea.location, dea.date, dea.population, vac.new_vaccinations,
SUM(CONVERT(bigint, vac.new_vaccinations)) over (partition by dea.location order by dea.location, dea.date 
ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) as totalvaccinations
FROM PortfolioProject..coviddeaths dea
join PortfolioProject..covidvaccinations vac
	on dea.location = vac.location
	and dea.date = vac.date
where dea.continent!=''

--Utilizing a CTE to execute calculations with a PARTITION BY clause in the prior query

with popvsvac (continent, location, date, population, new_vaccinations, totalvaccinations) as
(
SELECT dea.continent, dea.location, dea.date, dea.population, vac.new_vaccinations,
SUM(CONVERT(bigint, vac.new_vaccinations)) over (partition by dea.location order by dea.location, dea.date 
ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) as totalvaccinations
FROM PortfolioProject..coviddeaths dea
join PortfolioProject..covidvaccinations vac
	on dea.location = vac.location
	and dea.date = vac.date
where dea.continent!=''
)
SELECT *, (totalvaccinations/population) * 100 as persent_of_totalvaccination_by_population
FROM popvsvac 


-- Applying a temporary table to execute partitioned calculations in the previous query


DROP TABLE IF EXISTS PopulationVaccinated
CREATE TABLE PopulationVaccinated
(
continent nvarchar(225),
location nvarchar(225),
date datetime,
population numeric,
new_vaccinations numeric,
totalvaccinations numeric
)
INSERT INTO PopulationVaccinated
SELECT dea.continent, dea.location, dea.date, dea.population, vac.new_vaccinations,
SUM(CONVERT(bigint, vac.new_vaccinations)) over (partition by dea.location order by dea.location, dea.date 
ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) as totalvaccinations
FROM PortfolioProject..coviddeaths dea
join PortfolioProject..covidvaccinations vac
	on dea.location = vac.location
	and dea.date = vac.date;
	
-- Select data while filtering out rows with NULL values

SELECT *, (totalvaccinations/population) * 100 as persent_of_totalvaccination_by_population
FROM PopulationVaccinated
WHERE new_vaccinations IS NOT NULL;


-- Year-over-year growth in cases

WITH YearlyCases AS (
    SELECT location,
           DATEPART(YEAR, date) as year,
           SUM(new_cases) as Annual_total_cases
    FROM PortfolioProject..coviddeaths
    WHERE continent != ''
    GROUP BY location, DATEPART(YEAR, date)
),
YearlyGrowth AS (
    SELECT location, year,
           Annual_total_cases,
           LAG(Annual_total_cases) OVER (PARTITION BY location ORDER BY year) AS Cases_Recorded_Last_Year

    FROM YearlyCases
)
SELECT location, year, Annual_total_cases, Cases_Recorded_Last_Year,
       (Annual_total_cases - Cases_Recorded_Last_Year) AS growth_in_cases,
       ((Annual_total_cases - Cases_Recorded_Last_Year) / NULLIF(Cases_Recorded_Last_Year, 0)) * 100 AS growth_percentage
FROM YearlyGrowth
WHERE Cases_Recorded_Last_Year IS NOT NULL
ORDER BY location, year;

--Creating a View to Save Data for Future Visualization Needs

CREATE VIEW vw_year_over_year_growth AS
WITH YearlyCases AS (
    SELECT location,
           DATEPART(YEAR, date) AS year,
           SUM(new_cases) AS annual_total_cases
    FROM PortfolioProject..coviddeaths
    WHERE continent != ''
    GROUP BY location, DATEPART(YEAR, date)
),
YearlyGrowth AS (
    SELECT location, year,
           annual_total_cases,
           LAG(annual_total_cases) OVER (PARTITION BY location ORDER BY year) AS cases_recorded_last_year
    FROM YearlyCases
)
SELECT location, year, annual_total_cases, cases_recorded_last_year,
       (annual_total_cases - cases_recorded_last_year) AS growth_in_cases,
       ((annual_total_cases - cases_recorded_last_year) / NULLIF(cases_recorded_last_year, 0)) * 100 AS growth_percentage
FROM YearlyGrowth
WHERE cases_recorded_last_year IS NOT NULL;