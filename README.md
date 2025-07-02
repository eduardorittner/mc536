# MC536: Database Project - Energy production and consumption and education

This project was developed by
- Eduardo Rittner Coelho (250960)
- André Ribeiro do Valle Pereira (244185)
- André Macarini da Silva (246929)

This repository is home to our project on the MC536 course, which has two stages: A first one dealing with SQL databases (specifically PostgreSQL) and the second using No-SQL databases. We have chosen the [Barrolee](http://barrolee.com/) dataset on country education and scholarity levels and the [Our World in Energy Data](https://github.com/owid/energy-data) dataset on country energy production and consumption levels.

In the modern age, energy is more useful than ever, since anything remotely automated requires at least some form of computing, which in turn requires energy. Internet access, food production, product manufacturing, all require extensive energy use. In joining these datasets we hope to find out how closely education level and energy use are, and what steps countries can take to raise their population's education levels.

# How to run

## Dependencies

This project requires a python version greater than 3.13, and needs `psycopg2` to communicate with the postgres db, `pandas` to parse and process the datasets, `tqdm` to display progress and `argparse` to parse command line arguments. These dependencies are specified both in `pyproject.toml` and `requirements.txt`.

All the functionality is in db.py and is gated behind command-line flags. For a list of them, run `python3 bd.py --help`. The main ones are:

- `--clean`: Performs a clean start, resets the database schema and loads both datasets
- `--reset`: Only resets the database schema, not loading any data.
- `--energy`: Reload only the energy dataset.
- `--education`: Reload only the education dataset.
- `--queries`: Comma-delimited list of queries. For all queries use `--queries all`. In the energy database we modified some of the countries names(United States to USA and Vietnam ti Viet Nam). Most of the queries used data between 1965 and 2010, because that was the interval that matched between the two databases. The list of available queries is:
    - "education-variation"
    - "consumo-educacao"
    - "producao-educacao"
    - "correlacao-educacao-energia"
    - "education-disparity-energy"

The connection parameters can be configured via cli arguments as well:
- `--dbname`: Default is Educacao-e-energia
- `--user`: Default is postgres
- `--password`: Default is blank password
- `--host`: Default is localhost
- `--port`: Default is 5432


## Conceptual Model

This conceptual model describes how data related to countries, population demographics, education, and electricity is structured. Each País (Country) has many População (Population) records, representing individuals residing in that country by year, gender, and age group. Each population group is associated with one Escolaridade (Education) entry detailing the percentages of people by level of educational attainment, and one Média_Estudo entry containing average years of schooling (primary, secondary, and higher). The country also consumes or produces Eletricidade (Electricity), which is recorded by year and linked to a specific Tipo_Eletricidade (Electricity Type), such as wind, coal, or solar. This electricity is both consumed and produced by the country, and each energy entry is classified by its type. The model supports analysis of educational progress and energy dynamics over time across demographic segments and countries

![conceitual](https://github.com/user-attachments/assets/0bc0386f-f7c0-4131-a2a9-8e4c9b9eabe4)


## Relational Model

This relational database stores data on countries, populations, education, and energy. The Pais table lists countries, each linked to multiple entries in Populacao, which records demographic data by year, gender, and age group. Educational attainment for each population group is detailed in Escolaridade (percentages by level) and Media_Estudo (average years of schooling). Energy data is managed in Eletricidade, which logs production and consumption by country, year, and energy type—classified in Tipo_Eletricidade. This structure supports analyses of educational and energy trends across time and demographics.

![Relacional](https://github.com/user-attachments/assets/0e2407e8-920c-4f39-a477-83b4779e180e)


Or using the pgAdmin4

![relacional_sql_post](https://github.com/user-attachments/assets/39289697-6026-444a-b16a-f289609186c3)


## Physical Model
The physical model generated from pgAdmin4 can be found on this [link](https://github.com/eduardorittner/mc536/blob/main/entrega_sql/schema.sql)

## Logical Model
The logical model generated from pgAdmin4 can be found here

## Database Schema

The database schema is designed to store and manage information related to countries, population demographics, education, and electricity production and consumption. It includes six main tables with clearly defined relationships.

The "Pais" table stores a list of countries, each identified by a unique name and a primary key (id) that is auto-generated. The "Populacao" table records demographic information such as the year, gender, age group, and the number of individuals. Each population entry is linked to a specific country through the pais_id foreign key.

The "Escolaridade" table contains percentages of the population that have reached or completed various education levels, including no schooling, primary, secondary, and higher education. This table is linked to the population data through the populacao_id foreign key. Similarly, the "Media_Estudo" table stores the average number of years spent in primary, secondary, and higher education, as well as the overall average years of study, also linked to the population via populacao_id.

The "Tipo_Eletricidade" table defines the types of electricity (such as hydroelectric, fossil fuel, or renewable), and the "Eletricidade" table records data on electricity production and consumption for each country, year, and type of electricity. The electricity data references the country with pais_id and the type of electricity with tipo_eletricidade_id.

All primary keys are integers generated by default as identity values, and the foreign key constraints enforce referential integrity between related tables. This schema allows for comprehensive tracking and analysis of social and energy-related indicators over time and across countries.

## Queries
Five queries were developed to analyze each of the questions below:
1. Sort countries by energy consumption and show their education level
2. Percentage of Educational Attainment by Age Group in Brazil (2010) 
3. Rank countries that had the highest percentage increase in completion of secondary education and show their energy production
4. Find the energy source that has the highest Pearson coefficient with tertiary education
5. Countries currently(2010) ranked by lowest "Ecological Footprint per capita"((fossil_production + fossil_consumption) - (renowable_production +renowable_consumption) / population)

## Results
Results of the queries:
1. Countries with higher energy consumption also tend to have historically higher averages of years of schooling, although this also depends on the size of the population.
2. This query retrieves the distribution of educational attainment percentages (e.g., no schooling, primary, secondary, and higher education levels) for each age group in Brazil in the year 2010, ordered by age group.
3. By subtracting the values of the percentage of complete secondary schooling over a 15-year interval, we were able to identify the countries with the greatest variation in education from 1965 to 2010, and also show their variation in energy production.
4. Using the Pearson correlation to analyze the relationship between the average years of tertiary schooling attained and energy source production, we found that nuclear energy has the highest Pearson coefficient with higher education. This makes sense, as nuclear energy production requires high levels of technology. However, the coefficient still indicates only a weak correlation
5. Using this arbitrary equation to try to measure the ecological footprint, we found that the Scandinavian countries have the smallest footprints, while Arab countries that are major producers of oil and gas have the largest ecological footprints. This aligns with common knowledge. We believe this query is only relevant if we use the most recent data available, which is from 2010.
