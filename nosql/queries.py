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
    education_collection = db[education_collection_name]

    energy_projection = {"_id": 0, "year": 1}
    for metric in energy_metrics:
        energy_projection[metric] = 1

    pipeline = [
        # Match on start and end years
        {
            "$match": {
                "year": {"$in": [start_year, end_year]},
                # Get information for the entire population
                "agefrom": 15,
                "ageto": 999,
            }
        },
        # Group by country and get the education metrics for start/end years
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
        # Calculate percentage increase and handle missing/zero values
        {
            "$project": {
                "_id": 0,
                "country": "$_id",
                "start_edu_value": 1,
                "end_edu_value": 1,
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
        # Filter out countries where education_increase_pct could not be calculated
        {"$match": {"education_increase_pct": {"$ne": None}}},
        # Join with energy data for the start_year
        {
            "$lookup": {
                "from": energy_collection_name,
                "localField": "country",
                "foreignField": "country",
                "as": "energy_data",
                "pipeline": [
                    {"$match": {"year": end_year}},
                    {
                        "$project": energy_projection,
                    },
                ],
            }
        },
        # Unwind energy_data array, if a country has no energy data for end_year, it will be filtered out here.
        {"$unwind": "$energy_data"},
        {"$sort": {"education_increase_pct": -1}},
        # Limit to at most n_matches
        {"$limit": n_matches},
        # Final projection to shape the output
        {
            "$project": {
                "_id": 0,
                "country": 1,
                "education_increase_pct": 1,
                "end_year_energy_data": "$energy_data",  # Nest energy data for clarity
            }
        },
    ]

    results = list(education_collection.aggregate(pipeline))

    print(
        f"Ranking Countries by Highest Percentage Increase in '{education_metric}' ({start_year}-{end_year}):"
    )
    print("-" * 80)
    for i, doc in enumerate(results):
        print(f"Rank {i + 1}. Country: {doc.get('country')}")
        print(
            f"   Education Increase (%): {doc.get('education_increase_pct', 'N/A'):.2f}%"
        )
        energy_info = doc.get("end_year_energy_data", {})
        print(f"   Energy Data ({end_year}):")
        for metric in energy_metrics:
            value = energy_info.get(metric, "N/A")
            if isinstance(value, (int, float)):
                print(f"     {metric.replace('_', ' ').title()}: {value:,.2f}")
            else:
                print(f"     {metric.replace('_', ' ').title()}: {value}")


def sort_countries_by_energy_and_education(
    db, energy_collection_name, education_collection_name, year=2010
):
    """
    Connects to MongoDB, sorts countries by energy per capita,
    and displays relevant education levels for a specific year.
    """

    energy_collection = db[energy_collection_name]

    pipeline = [
        {
            # Match documents with the given year
            "$match": {"year": year}
        },
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
                            # Get education data for ages 15 and up
                            "agefrom": 15,
                            "ageto": 999,
                        }
                    },
                    {
                        "$project": {
                            "_id": 0,
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
        {
            # Unwind the education_data array.
            # since we expect only one education document per country per year and age range.
            # If no education data, the document will be dropped by default.
            "$unwind": "$education_data"
        },
        {
            # Sort by energy per capita in descending order
            "$sort": {"energy_per_capita": -1}
        },
        {
            # Project the final desired fields
            "$project": {
                "_id": 0,
                "country": 1,
                "year": 1,
                "energy_per_capita": 1,
                "education_level": "$education_data",
            }
        },
    ]

    results = list(energy_collection.aggregate(pipeline))

    for doc in results:
        print(f"Country: {doc.get('country')}, Year: {doc.get('year')}")
        print(f"  Energy Per Capita: {doc.get('energy_per_capita'):,.2f} ")
        if doc.get("education_level"):
            edu = doc["education_level"]
            print(f"  Education Level:")
            print(
                f"    Avg. Schooling Years: {edu.get('schooling_years_avg', 'N/A'):.2f}"
            )
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
        else:
            print("  Education Level: N/A (data not found for this year/country)")
        print("-" * 30)


uri = "mongodb://localhost:27017/"
client = MongoClient(uri)
db = client["education_and_energy"]
# sort_countries_by_energy_and_education(db, "energy", "education", year=2010)
rank_countries_by_education_increase(
    db,
    "energy",
    "education",
    EducationIncreaseMetric.AVG_SCHOOL_YEARS.value,
    1990,
    1995,
    ["energy_per_capita", "oil_electricity"],
    10,
)
