from pymongo import MongoClient
from enum import Enum


class EducationIncreaseMetric(Enum):
    AVG_SCHOOL_YEARS = "schooling_years_avg"
    PRIMARY_COMPLETE = "primary_schooling_complete"
    SECONDARY_COMPLETE = "secondary_schooling_complete"
    TERTIARY_COMPLETE = "tertiary_schooling_complete"


def rank_countries_by_education_increase(
    db,
    energy_collection_name,
    education_collection_name,
    countries_collection_name,
    education_metric,
    start_year,
    end_year,
    energy_metrics,
    limit=10,
):
    education_collection = db[education_collection_name]
    countries = [doc["country"] for doc in db[countries_collection_name].find()]

    energy_increase_projection = {}
    for metric_string in energy_metrics:
        metric_type, metric_name = metric_string.rsplit("_", 1)
        energy_increase_projection[metric_string] = {
            "$let": {
                "vars": {
                    "start_val": {"$arrayElemAt": [f"$start_docs.{metric_type}", 0]},
                    "end_val": {"$arrayElemAt": [f"$end_docs.{metric_type}", 0]},
                },
                "in": {
                    "$cond": [
                        {
                            "$and": [
                                {"$ne": ["$$start_val", None]},
                                {"$ne": ["$$end_val", None]},
                                {"$ne": ["$$start_val", 0]},
                            ]
                        },
                        {
                            "$multiply": [
                                {
                                    "$divide": [
                                        {"$subtract": ["$$end_val", "$$start_val"]},
                                        "$$start_val",
                                    ]
                                },
                                100,
                            ]
                        },
                        None,
                    ]
                },
            }
        }

    pipeline = [
        # Education calculation stages remain the same
        {
            "$match": {
                "country": {"$in": countries},
                "year": {"$in": [start_year, end_year]},
                "agefrom": 15,
                "ageto": 999,
            }
        },
        {
            "$group": {
                "_id": "$country",
                "start_edu_value": {
                    "$max": {
                        "$cond": [
                            {"$eq": ["$year", start_year]},
                            f"${education_metric}",
                            None,
                        ]
                    }
                },
                "end_edu_value": {
                    "$max": {
                        "$cond": [
                            {"$eq": ["$year", end_year]},
                            f"${education_metric}",
                            None,
                        ]
                    }
                },
            }
        },
        {
            "$project": {
                "_id": 0,
                "country": "$_id",
                "education_increase_pct": {
                    "$cond": [
                        {
                            "$and": [
                                {"$ne": ["$start_edu_value", 0]},
                                {"$ne": ["$start_edu_value", None]},
                                {"$ne": ["$end_edu_value", None]},
                            ]
                        },
                        {
                            "$multiply": [
                                {
                                    "$divide": [
                                        {
                                            "$subtract": [
                                                "$end_edu_value",
                                                "$start_edu_value",
                                            ]
                                        },
                                        "$start_edu_value",
                                    ]
                                },
                                100,
                            ]
                        },
                        None,
                    ]
                },
            }
        },
        {"$match": {"education_increase_pct": {"$ne": None}}},
        {"$sort": {"education_increase_pct": -1}},
        {
            "$lookup": {
                "from": energy_collection_name,
                "let": {
                    "country_name": "$country",
                    "start": start_year,
                    "end": end_year,
                },
                "pipeline": [
                    {
                        "$match": {
                            "$expr": {
                                "$and": [
                                    {"$eq": ["$country", "$$country_name"]},
                                    {"$in": ["$year", ["$$start", "$$end"]]},
                                ]
                            }
                        }
                    }
                ],
                "as": "energy_data",
            }
        },
        {
            "$addFields": {
                "start_docs": {
                    "$filter": {
                        "input": "$energy_data",
                        "as": "d",
                        "cond": {"$eq": ["$$d.year", start_year]},
                    }
                },
                "end_docs": {
                    "$filter": {
                        "input": "$energy_data",
                        "as": "d",
                        "cond": {"$eq": ["$$d.year", end_year]},
                    }
                },
            }
        },
        {"$addFields": {"energy_metrics_increase_pct": energy_increase_projection}},
        {
            "$project": {
                "_id": 0,
                "country": 1,
                "education_increase_pct": 1,
                "energy_metrics_increase_pct": 1,
            }
        },
        {"$limit": limit},
    ]

    results = list(education_collection.aggregate(pipeline))

    print(
        f"Ranking Countries by Highest Percentage Increase in '{education_metric}' ({start_year}-{end_year}):"
    )
    print("-" * 80)

    if not results:
        print("No countries found matching the specified criteria.")
        return []

    for i, doc in enumerate(results):
        print(f"Rank {i + 1}. Country: {doc.get('country')}")
        print(
            f"  Education Increase (%): {doc.get('education_increase_pct', 'N/A'):.2f}%"
        )
        energy_increases = doc.get("energy_metrics_increase_pct", {})
        print(f"  Energy Metrics Percent Increase ({start_year}-{end_year}):")
        if not energy_increases:
            print("    No energy data available for the specified period.")
            continue

        for metric, increase in energy_increases.items():
            metric_name = metric.replace("_", " ").title()
            if increase is not None:
                print(f"    {metric_name}: {increase:,.2f}%")
            else:
                print(f"    {metric_name}: N/A")


def sort_countries_by_energy_and_education(
    db,
    energy_collection_name,
    education_collection_name,
    countries_collection_name,
    limit=10,
    year=2010,
):
    countries = [doc["country"] for doc in db[countries_collection_name].find()]

    pipeline = [
        {
            "$match": {
                "year": year,
                "country": {"$in": countries}
            }
        },
        {
            "$group": {
                "_id": "$country",
                "total_consumption": {"$sum": "$consumption"},
                "year": {"$first": "$year"},  # Keep the year field
            }
        },
        {
            "$lookup": {
                "from": education_collection_name,
                "localField": "_id",
                "foreignField": "country",
                "as": "education_data",
                "pipeline": [
                    {
                        "$match": {
                            "year": year,
                            "agefrom": 15,
                            "ageto": 999
                        }
                    },
                    {
                        "$project": {
                            "_id": 0,
                            "population": 1,
                            "no_schooling": 1,
                            "primary_schooling_complete": 1,
                            "secondary_schooling_complete": 1,
                            "tertiary_schooling_complete": 1,
                            "schooling_years_avg": 1,
                        }
                    },
                ],
            }
        },
        {"$unwind": "$education_data"},
        {"$sort": {"total_consumption": -1}},
        {"$limit": limit},
        {
            "$project": {
                "_id": 0,
                "country": "$_id",
                "year": 1,
                "total_consumption": 1,
                "education_level": "$education_data",
            }
        },
    ]

    energy_collection = db[energy_collection_name]
    results = list(energy_collection.aggregate(pipeline))

    if not results:
        print("⚠️ No countries with total consumption data found.")
        return

    for doc in results:
        print(f"Country: {doc.get('country')}, Year: {doc.get('year')}")
        total = doc.get("total_consumption")
        print(
            f"  Total Consumption: {total:,.2f} TWh"
            if total is not None
            else "  Total Consumption: N/A"
        )

        edu = doc.get("education_level", {})
        print("  Education Level:")
        print(f"    Avg. Schooling Years: {edu.get('schooling_years_avg', 'N/A'):.2f}")
        print(f"    No Schooling (%): {edu.get('no_schooling', 'N/A'):.2f}%")
        print(
            f"    Primary Complete (%): {edu.get('primary_schooling_complete', 'N/A'):.2f}%"
        )
        print(
            f"    Secondary Complete (%): {edu.get('secondary_schooling_complete', 'N/A'):.2f}%"
        )
        print(
            f"    Tertiary Complete (%): {edu.get('tertiary_schooling_complete', 'N/A'):.2f}%"
        )
        print("-" * 40)

def get_education_distribution_by_age(db, country, year, limit=10):
    pipeline = [
        {"$match": {"country": country, "year": year}},
        {"$project": {
            "agefrom": 1,
            "ageto": 1,
            "no_schooling": 1,
            "primary_schooling": 1,
            "primary_schooling_complete": 1,
            "secondary_schooling": 1,
            "secondary_schooling_complete": 1,
            "tertiary_schooling": 1,
            "tertiary_schooling_complete": 1
        }},
        {"$addFields": {
            "total": {
                "$add": [
                    {"$ifNull": ["$no_schooling", 0]},
                    {"$ifNull": ["$primary_schooling", 0]},
                    {"$ifNull": ["$secondary_schooling", 0]},
                    {"$ifNull": ["$tertiary_schooling", 0]}
                ]
            }
        }},
        {"$project": {
            "agefrom": 1,
            "ageto": 1,
            "no_schooling_pct": {
                "$cond": [{"$gt": ["$total", 0]},
                          {"$multiply": [{"$divide": ["$no_schooling", "$total"]}, 100]},
                          0]
            },
            "primary_pct": {
                "$cond": [{"$gt": ["$total", 0]},
                          {"$multiply": [{"$divide": ["$primary_schooling", "$total"]}, 100]},
                          0]
            },
            "secondary_pct": {
                "$cond": [{"$gt": ["$total", 0]},
                          {"$multiply": [{"$divide": ["$secondary_schooling", "$total"]}, 100]},
                          0]
            },
            "tertiary_pct": {
                "$cond": [{"$gt": ["$total", 0]},
                          {"$multiply": [{"$divide": ["$tertiary_schooling", "$total"]}, 100]},
                          0]
            }
        }},
        {"$sort": {"agefrom": 1}},
        {"$limit": limit},
    ]

    results = list(db.education.aggregate(pipeline))
    
    print(f"\nDistribuição educacional em {country}, {year}:\n")
    for row in results:
        print(f"{row['agefrom']}-{row['ageto']} anos:")
        print(f"  Sem escolaridade: {row['no_schooling_pct']:.1f}%")
        print(f"  Ensino primário: {row['primary_pct']:.1f}%")
        print(f"  Ensino secundário: {row['secondary_pct']:.1f}%")
        print(f"  Ensino superior: {row['tertiary_pct']:.1f}%\n")

    return results

def ecological_footprint_ranking(
    db,
    energy_collection_name,
    education_collection_name,
    countries_collection_name,
    year,
    limit=10,
):
    countries_collection = db[countries_collection_name]

    pipeline = [
        # 1. Start with the base list of countries.
        # The '$project' stage ensures we only pass the country name down the pipeline.
        {"$project": {"country": 1, "_id": 0}},
        # 2. Look up all related energy documents for the given year.
        # With the new schema, this will return an array of documents,
        # one for each energy type (oil, hydro, etc.).
        {
            "$lookup": {
                "from": energy_collection_name,
                "let": {"country_name": "$country"},
                "pipeline": [
                    {
                        "$match": {
                            "$expr": {
                                "$and": [
                                    {"$eq": ["$country", "$$country_name"]},
                                    {"$eq": ["$year", year]},
                                ]
                            }
                        }
                    }
                ],
                "as": "energy_data",
            }
        },
        # 3. Look up population data from the education collection.
        {
            "$lookup": {
                "from": education_collection_name,
                "let": {"country_name": "$country"},
                "pipeline": [
                    {
                        "$match": {
                            "$expr": {
                                "$and": [
                                    {"$eq": ["$country", "$$country_name"]},
                                    {"$eq": ["$year", year]},
                                    {"$eq": ["$agefrom", 15]},
                                    {"$eq": ["$ageto", 999]},
                                ]
                            }
                        }
                    },
                    {"$project": {"population": 1, "_id": 0}},
                ],
                "as": "population_data",
            }
        },
        {"$unwind": {"path": "$population_data"}},
        # 4. Calculate all energy totals in a single, compact stage.
        # This stage processes the 'energy_data' array from the lookup.
        {
            "$addFields": {
                "population": "$population_data.population",
                "non_renewable_total": {
                    "$sum": {
                        "$map": {
                            "input": "$energy_data",
                            "as": "e",
                            "in": {
                                "$cond": [
                                    {
                                        "$in": [
                                            "$$e.type",
                                            ["oil", "coal", "gas"],
                                        ]
                                    },
                                    {
                                        "$add": [
                                            {"$ifNull": ["$$e.production", 0]},
                                            {"$ifNull": ["$$e.consumption", 0]},
                                        ]
                                    },
                                    0,
                                ]
                            },
                        }
                    }
                },
                "renewable_total": {
                    "$sum": {
                        "$map": {
                            "input": "$energy_data",
                            "as": "e",
                            "in": {
                                "$cond": [
                                    {
                                        "$in": [
                                            "$$e.type",
                                            [
                                                "hydro",
                                                "wind",
                                                "solar",
                                                "nuclear",
                                                "other_renewable",
                                            ],
                                        ]
                                    },
                                    {
                                        "$add": [
                                            {"$ifNull": ["$$e.production", 0]},
                                            {"$ifNull": ["$$e.consumption", 0]},
                                        ]
                                    },
                                    0,
                                ]
                            },
                        }
                    }
                },
            }
        },
        # 5. Calculate the final per-capita value.
        {
            "$addFields": {
                "ecological_footprint_per_capita": {
                    "$cond": [
                        {"$gt": ["$population", 0]},
                        {
                            "$divide": [
                                {
                                    "$subtract": [
                                        "$non_renewable_total",
                                        "$renewable_total",
                                    ]
                                },
                                "$population",
                            ]
                        },
                        None,
                    ]
                }
            }
        },
        # 6. Project the final fields and sort the results.
        {
            "$project": {
                "country": 1,
                "ecological_footprint_per_capita": 1,
            }
        },
        {"$sort": {"ecological_footprint_per_capita": 1}},
        {"$limit": limit},
        ]

    results = list(countries_collection.aggregate(pipeline))

    print(
        f"\nRanking of countries by lowest ecological footprint per capita for the year {year}:\n"
    )
    for rank, row in enumerate(results, 1):
        ef = row.get("ecological_footprint_per_capita")
        ef_display = f"{ef:.6f}" if ef is not None else "N/A"
        print(
            f"{rank:2}. {row.get('country', 'N/A'):25} -> Per Capita Footprint: {ef_display}"
        )

    return results

def top_countries_by_age_and_schooling(
    db,
    energy_collection_name,
    education_collection_name,
    year,
    age_from: int,
    age_to: int,
    limit: int = 10
):
    education_collection = db[education_collection_name]

    pipeline = [
        {
            "$match": {
                "year": year,
                "agefrom": age_from,
                "ageto": age_to,
                "schooling_years_avg": {"$gt": 0}
            }
        },
        {
            "$lookup": {
                "from": energy_collection_name,
                "let": {
                    "country_name": "$country",
                    "year_val": "$year"
                },
                "pipeline": [
                    {
                        "$match": {
                            "$expr": {
                                "$and": [
                                    {"$eq": ["$country", "$$country_name"]},
                                    {"$eq": ["$year", "$$year_val"]},
                                    {"$gt": ["$production", 0]}
                                ]
                            }
                        }
                    },
                    {"$project": {"_id": 0, "production": 1}}
                ],
                "as": "energy_data"
            }
        },
        {
            "$project": {
                "_id": 0,
                "country": 1,
                "year": 1,
                "agefrom": 1,
                "ageto": 1,
                "schooling_years_avg": 1,
                "total_energy_production": {"$sum": "$energy_data.production"}
            }
        },
        {
            "$match": {
                "total_energy_production": {"$gt": 0}
            }
        },
        {"$sort": {"schooling_years_avg": -1}},
        {"$limit": limit}
    ]

    resultados = education_collection.aggregate(pipeline)

    print(f"\nTop {limit} países para faixa etária {age_from}-{age_to} (Improved Query):\n")
    for doc in resultados:
        print(f"- {doc['country']} ({doc['year']}): idade {doc['agefrom']}-{doc['ageto']}, "
              f"escolaridade média = {doc['schooling_years_avg']:.2f}, "
              f"produção total de energia = {doc.get('total_energy_production', 0):.2f}")

uri = "mongodb://localhost:27017/"
client = MongoClient(uri)
db = client["education_and_energy"]

# Query Examples:
# Uncomment the code and modify values

# rank_countries_by_education_increase(
#     db,
#     "energy",
#     "education",
#     "countries",
#     EducationIncreaseMetric.AVG_SCHOOL_YEARS.value,
#     1990,
#     1995,
#     ["oil_consumption"],
#     10,
# )

#sort_countries_by_energy_and_education(db, "energy", "education", "countries", 10)

#get_education_distribution_by_age(db, "Brazil", 2010, 10)

#ecological_footprint_ranking(db, "energy", "education", "countries", 1990, 5)

top_countries_by_age_and_schooling(db, "energy", "education", 1990, age_from=15, age_to=999, limit=5)
