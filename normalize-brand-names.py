import firebase_admin
import numpy as np
import pandas as pd

from config import BUSINESS_NAME_BASELINE_SCORE, COMMON_BUSINESS_NAMES, FilePaths
from firebase_admin import credentials
from firebase_admin import firestore
from fuzzywuzzy import fuzz
from fuzzywuzzy import process


# Create firestore link
cred = credentials.Certificate(FilePaths.FIREBASE_KEY_PATH)
firebase_admin.initialize_app(cred)
db = firestore.client()


# String comparison
def compare(x, y):
    return fuzz.WRatio(x, y)


# Returns list with objects of keys EntryId, EntryOwnerId, and TimePosted
def get_all_feed_entries() -> list[dict]:
    feed_ref = db.collection(u'Feed')
    feed_docs = feed_ref.stream()
    feed_entries = []
    for doc in feed_docs:
        feed_entries.append(doc.to_dict())
    return feed_entries


# Normalizes all business names in the database
def normalize_business_names():

    # Get feed entries
    feed_entries = get_all_feed_entries()

    # Iterate feed entries
    users_ref = db.collection(u'LinkinBioUsers')
    for i in range(0, len(feed_entries)):

        # Get post info dictionary
        entry = feed_entries[i]
        user_ref = users_ref.document(entry['EntryOwnerId'])
        post_ref = user_ref.collection(u'portfolio').document(entry['EntryId'])
        post_info = post_ref.get().to_dict()

        # Check if document attribute "BusinessName" exists
        if "BusinessName" in post_info:
            business_name = post_info["BusinessName"]
            closest_match = process.extractOne(business_name, COMMON_BUSINESS_NAMES)

            # Update BusinessName if there is a valid match
            new_name = closest_match[0]
            if closest_match[1] >= BUSINESS_NAME_BASELINE_SCORE and new_name is not business_name:
                post_ref.update({u'BusinessName': new_name})
                print(f"Updated '{business_name}' to '{new_name}' (User: {entry['EntryOwnerId']}, Post: {entry['EntryId']})")


# Fuzzywuzzy comparisons of all raw business names with all normalized names
def business_normalization_benchmark():

    # Get raw business names
    business_names = pd.read_csv("./data/business_names_raw.csv")
    business_names = business_names['BusinessName'].tolist()
    business_names.sort()

    # Create name matrix
    matrix = pd.DataFrame(
        index=business_names,
        columns=COMMON_BUSINESS_NAMES
    )
    
    # Generate comparison matrix and output to csv
    name_comparisons = matrix.apply(
        lambda x: pd.DataFrame(x).apply(lambda y: compare(x.name, y.name), axis=1)
    )
    name_comparisons.to_csv("./data/business_data/business_name_comparisons.csv", encoding='utf-8', index=True)


if __name__ == "__main__":
    # business_normalization_benchmark()
    normalize_business_names()
    exit()
    


