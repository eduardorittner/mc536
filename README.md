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
- `--queries`: Comma-delimited list of queries. For all queries use `--queries all`. The list of available queries is:
    - "education-variation": Finds the countries with highest variation in average years of schooling from 2000 to 2010 and displays their energy production levels
    - "consumo-educacao"
    - "producao-educacao"
    - "correlacao"

The connection parameters can be configured via cli arguments as well:
- `--dbname`: Default is Educacao-e-energia
- `--user`: Default is postgres
- `--password`: Default is blank password
- `--host`: Default is localhost
- `--port`: Default is 5432


## Conceptual Model

This conceptual model represents data on demographics, education, and energy production by country and year. Each country has multiple populations, and each population—defined by year, gender, age group, and size—is linked to a country and may include associated educational attainment percentages and average years of study. Energy production data is recorded by year and country, including various sources such as solar, wind, gas, hydro, and nuclear. These sources are represented in the “Fonte” entity, which connects each energy type to its corresponding energy and electricity data, including production, consumption, and annual variation. The model ensures that every population is tied to a specific country, and every production entry is connected to a country and optionally to energy and electricity records through sources.

![conceitual](https://github.com/user-attachments/assets/e9b86bf9-4de0-4f5a-836a-334b4b4ba3be)

## Relational Model

The relational model reflects a normalized and modular structure that captures the connection between countries, their population demographics, education indicators, and energy production metrics. At its core, each country (Pais) is linked to multiple population groups (Populacao), segmented by year, sex, and age range. Each population group can then be analyzed educationally via two tables: Media_Estudo, which stores average years of schooling, and Escolaridade, which contains detailed percentages of educational attainment. On the energy side, each country and year combination in Producao references multiple energy sources, each of which connects to a Fonte—a bridge to both Energia (production/consumption data) and Eletricidade (electricity-specific data). This structure ensures analytical flexibility, enabling queries that associate energy profiles with educational development across time and geography.

![Relacional](https://github.com/user-attachments/assets/77e408ee-99a6-4d06-b826-5374cfd8c4d7)


## Physical Model

## Database Schema

Our database schema is designed to reflect the relationship between a country's population, its education levels, and its energy production and consumption. It was structured to accommodate both datasets in a normalized and scalable way, while preserving referential integrity between all key metrics.

At the center of the schema is the "Pais" table, which contains the unique list of countries present in both datasets. The "Populacao" table captures demographic data per country, year, gender, and age group, and is linked directly to "Pais". For each population group, we store education-related statistics in two linked tables: "Media_Estudo" (average years of schooling) and "Escolaridade" (distribution across primary, secondary, and higher education levels).

The energy-related data is structured around the "Producao" table, which aggregates annual production metrics per country, broken down by energy source (e.g., wind, solar, coal). Each production entry points to multiple "Fonte" records, which combine information from both "Energia" (e.g., total production/consumption) and "Eletricidade" (e.g., electricity output and share). This modular design allows us to analyze both total energy trends and electricity-specific insights for each energy type.

By connecting these domains through foreign keys, the schema enables complex queries that link education metrics with energy variables — such as identifying correlations between investment in renewables and increases in schooling, or ranking countries by energy consumption relative to their education levels.
