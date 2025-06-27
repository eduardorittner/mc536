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
    # Note that education metric should be the string value
    # So pass in enum.value
    education_metric,
    start_year,
    end_year,
    energy_metrics,
    n_matches,
):
    """
    Ranks countries by the highest percentual increase in a specific education metric
    over a given period and returns the percentual increase in various energy metrics
    over the same period for those countries.

    Filters out countries for which NO energy metric increases could be calculated.
    """
    education_collection = db[education_collection_name]

    # Dynamically construct the projection for calculating energy metric increases
    energy_increase_projection = {}
    for metric in energy_metrics:
        energy_increase_projection[metric] = {
            "$cond": [
                {
                    "$and": [
                        {"$ne": [f"$start_energy_data.{metric}", None]},
                        {"$ne": [f"$end_energy_data.{metric}", None]},
                        {"$ne": [f"$start_energy_data.{metric}", 0]},
                    ]
                },
                {
                    "$multiply": [
                        {
                            "$divide": [
                                {
                                    "$subtract": [
                                        f"$end_energy_data.{metric}",
                                        f"$start_energy_data.{metric}",
                                    ]
                                },
                                f"$start_energy_data.{metric}",
                            ]
                        },
                        100,
                    ]
                },
                None,  # Result if conditions are not met
            ]
        }

    # Dynamically construct the $or condition for the new $match stage
    # This ensures at least one energy metric could be calculated.
    filter_valid_energy_data_conditions = [
        {f"energy_metrics_increase_pct.{metric}": {"$ne": None}}
        for metric in energy_metrics
    ]

    pipeline = [
        # Match on start and end years in the education collection
        {
            "$match": {
                "year": {"$in": [start_year, end_year]},
                "agefrom": 15,
                "ageto": 999,
            }
        },
        # Group by country to get education values for start and end years
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
        # Calculate the percentage increase in education
        {
            "$project": {
                "_id": 0,
                "country": "$_id",
                "education_increase_pct": {
                    "$cond": [
                        {
                            "$and": [
                                {"$ne": ["$start_edu_value", None]},
                                {"$ne": ["$end_edu_value", None]},
                                {"$ne": ["$start_edu_value", 0]},
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
        # Filter out countries where education increase could not be calculated
        {"$match": {"education_increase_pct": {"$ne": None}}},
        # Sort by the highest education increase before the lookup for efficiency
        {"$sort": {"education_increase_pct": -1}},
        # Join with energy data for both start and end years
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
                                    {"$in": ["$year", [start_year, end_year]]},
                                ]
                            }
                        }
                    },
                ],
                "as": "energy_data",
            }
        },
        # Create fields for start and end year energy data
        {
            "$addFields": {
                "start_energy_data": {
                    "$arrayElemAt": [
                        {
                            "$filter": {
                                "input": "$energy_data",
                                "as": "data",
                                "cond": {"$eq": ["$$data.year", start_year]},
                            }
                        },
                        0,
                    ]
                },
                "end_energy_data": {
                    "$arrayElemAt": [
                        {
                            "$filter": {
                                "input": "$energy_data",
                                "as": "data",
                                "cond": {"$eq": ["$$data.year", end_year]},
                            }
                        },
                        0,
                    ]
                },
            }
        },
        # Calculate percentual increase for each energy metric
        {"$addFields": {"energy_metrics_increase_pct": energy_increase_projection}},
        # ***** NEW STAGE: Filter out countries where ALL energy metrics are null *****
        {"$match": {"$or": filter_valid_energy_data_conditions}},
        # Final projection to shape the output
        {
            "$project": {
                "_id": 0,
                "country": 1,
                "education_increase_pct": 1,
                "energy_metrics_increase_pct": 1,
            }
        },
        {"$limit": n_matches},
    ]

    # Handle the edge case where no energy metrics are requested
    if not energy_metrics:
        # Find the stage with the "$or" filter and remove it
        pipeline = [stage for stage in pipeline if "$or" not in stage.get("$match", {})]

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
    db, energy_collection_name, education_collection_name, n_matches, year=2010
):
    """
    Ordena países pelo consumo total de energia no ano especificado
    e exibe dados educacionais associados.
    """

    consumption_fields = [
        "coal_consumption",
        "solar_consumption",
        "wind_consumption",
        "gas_consumption",
        "hydro_consumption",
        "nuclear_consumption",
        "oil_consumption",
        "other_renewable_consumption",
    ]

    total_consumption_expr = {
        "$add": [{"$ifNull": [f"${field}", 0]} for field in consumption_fields]
    }

    pipeline = [
        {"$match": {"year": year}},
        {
            "$lookup": {
                "from": education_collection_name,
                "localField": "country",
                "foreignField": "country",
                "as": "education_data",
                "pipeline": [
                    {
                        "$match": {
                            "year": year,
                            "agefrom": 15,
                            "ageto": 999,
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
        {"$addFields": {"total_consumption": total_consumption_expr}},
        {"$match": {"total_consumption": {"$ne": None}}},
        {"$sort": {"total_consumption": -1}},
        {"$limit": n_matches},
        {
            "$project": {
                "_id": 0,
                "country": 1,
                "year": 1,
                "total_consumption": 1,
                "education_level": "$education_data",
            }
        },
    ]

    energy_collection = db[energy_collection_name]
    results = list(energy_collection.aggregate(pipeline))

    if not results:
        print("⚠️ Nenhum país com dados de consumo total encontrados.")
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

def get_education_distribution_by_age(db, country, year):
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
        {"$sort": {"agefrom": 1}}
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

def ecological_footprint_ranking(db, year):
    pipeline = [
        {"$match": {"year": year}},
        {"$lookup": {
            "from": "education",  # para pegar a população
            "let": {"country_name": "$country", "yr": "$year"},
            "pipeline": [
                {"$match": {
                    "$expr": {
                        "$and": [
                            {"$eq": ["$country", "$$country_name"]},
                            {"$eq": ["$year", "$$yr"]},
                            {"$eq": ["$agefrom", 15]},
                            {"$eq": ["$ageto", 999]}  # população total
                        ]
                    }
                }},
                {"$project": {"population": 1, "_id": 0}}
            ],
            "as": "population_data"
        }},
        {"$unwind": "$population_data"},
        {"$addFields": {
            "population": "$population_data.population",
            "fossil_production": {
                "$add": [
                    {"$ifNull": ["$oil_electricity", 0]},
                    {"$ifNull": ["$coal_electricity", 0]},
                    {"$ifNull": ["$gas_electricity", 0]}
                ]
            },
            "fossil_consumption": {
                "$add": [
                    {"$ifNull": ["$oil_consumption", 0]},
                    {"$ifNull": ["$coal_consumption", 0]},
                    {"$ifNull": ["$gas_consumption", 0]}
                ]
            },
            "renewable_production": {
                "$add": [
                    {"$ifNull": ["$hydro_electricity", 0]},
                    {"$ifNull": ["$wind_electricity", 0]},
                    {"$ifNull": ["$solar_electricity", 0]},
                    {"$ifNull": ["$nuclear_electricity", 0]},
                    {"$ifNull": ["$other_renewable_electricity", 0]}
                ]
            },
            "renewable_consumption": {
                "$add": [
                    {"$ifNull": ["$hydro_consumption", 0]},
                    {"$ifNull": ["$wind_consumption", 0]},
                    {"$ifNull": ["$solar_consumption", 0]},
                    {"$ifNull": ["$nuclear_consumption", 0]},
                    {"$ifNull": ["$other_renewable_consumption", 0]}
                ]
            }
        }},
        {"$addFields": {
            "ecological_footprint_per_capita": {
                "$cond": [
                    {"$gt": ["$population", 0]},
                    {
                        "$divide": [
                            {"$subtract": [
                                {"$add": ["$fossil_production", "$fossil_consumption"]},
                                {"$add": ["$renewable_production", "$renewable_consumption"]}
                            ]},
                            "$population"
                        ]
                    },
                    None
                ]
            }
        }},
        {"$project": {
            "country": 1,
            "ecological_footprint_per_capita": 1
        }},
        {"$sort": {"ecological_footprint_per_capita": 1}}  # Menor impacto primeiro
    ]

    results = list(db.energy.aggregate(pipeline))

    print(f"\nRanking de países por menor pegada ecológica per capita no ano {year}:\n")
    for rank, row in enumerate(results, 1):
        ef = row['ecological_footprint_per_capita']
        ef_display = f"{ef:.6f}" if ef is not None else "N/A"
        print(f"{rank:2}. {row['country']:25} -> Pegada per capita: {ef_display}")

    return results
    

uri = "mongodb://localhost:27017/"
client = MongoClient(uri)
db = client["education_and_energy"]


sort_countries_by_energy_and_education(db, "energy", "education", 10)
