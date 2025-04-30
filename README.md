# MC536: Database Project - Energy production and consumption and education

This project was developed by
- Eduardo Rittner Coelho (250960)o

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
    - "education-variation"


## Conceptual Model

## Relational Model

## Physical Model

## Database Schema
