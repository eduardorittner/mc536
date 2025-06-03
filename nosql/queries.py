from pymongo import MongoClient


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
sort_countries_by_energy_and_education(db, "energy", "education", year=2010)
