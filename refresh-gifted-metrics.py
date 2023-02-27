import firebase_admin
import json
import os
import pandas as pd
from datetime import datetime
from firebase_admin import credentials
from firebase_admin import firestore
from fuzzywuzzy import fuzz
from fuzzywuzzy import process
from config import FilePaths, COMMON_BUSINESS_NAMES, BUSINESS_NAME_BASELINE_SCORE

cred = credentials.Certificate(FilePaths.FIREBASE_KEY_PATH)
firebase_admin.initialize_app(cred)
db = firestore.client()

FEED_COLUMN_NAMES = ['TimePosted_TIMESTAMP', 'UserName', 'TimePosted', 'TimeUpdated', 'DealStarted', 'DealEnded', 'BusinessName', 'BusinessCategory',
                'ProductPromoted', 'PlatformPosted', 'BrandReview', 'ProductQuality', 'Recommendation', 'PaymentType', 'Tea']
USER_COLUMN_NAMES = ['UserName', 'Firstname', 'Lastname', 'Title', 'State', 'City', 'Gender', 'Category', 'Ethnicity', 'PhoneNumber', 'Email',
                    'Instagram_Username', 'Instagram_URL', 'TikTok_Username', 'TikTok_URL', 'Twitter_Username', 'Twitter_URL',
                    'LinkinBio_Visits_Count', 'Portfolio_Visits_Count', 'LinkinBio_Links_Count', 'Total_LinkinBio_Link_Click_Conversions']
PAYMENT_TYPES = ['None', 'Paid Partnership', 'Product Gifting', 'Both']
PLATFORM_TYPES = ['None', 'Instagram', 'Twitter', 'TikTok', 'Youtube']

FEED_DATA = []
FEED_DATA_DF = pd.DataFrame()
USER_DATA = []
KEY_METRICS = dict(
    total_user_count=0,
    total_deal_count=0,
    total_brand_count=0
)


# Returns list with objects of keys EntryId, EntryOwnerId, and TimePosted
def get_all_feed_entries() -> list[dict]:
    feed_ref = db.collection(u'Feed')
    feed_docs = feed_ref.stream()
    feed_entries = []
    for doc in feed_docs:
        feed_entries.append(doc.to_dict())
    return feed_entries


# Format enums
def format_enums(base_data, column_name, enum_list):
    logged_types = base_data[column_name]
    for i in range(0, len(logged_types)):
        logged_types[i] = enum_list[logged_types[i]]
    base_data[column_name] = ','.join(logged_types)
    return base_data


# Formats data
def format_feed_data(basic_info, secret_info):

    # Format timestamp
    secret_info['TimePosted_TIMESTAMP'] = secret_info['TimePosted'].timestamp()
    secret_info['TimePosted'] = secret_info['TimePosted'].ctime()
    secret_info['TimeUpdated'] = secret_info['TimeUpdated'].ctime()
    basic_info['DealStarted'] = basic_info['DealStarted'].ctime()
    if "DealEnded" in basic_info:
        basic_info['DealEnded'] = basic_info['DealEnded'].ctime()

    # Format enums
    basic_info = format_enums(basic_info, 'PlatformPosted', PLATFORM_TYPES)
    secret_info = format_enums(secret_info, 'PaymentType', PAYMENT_TYPES)
    
    return basic_info, secret_info


# Returns all information associated with feed posts
def get_feed_data():
    feed_data = []

    # Get feed entries
    feed_entries = get_all_feed_entries()

    # Iterate feed entries
    users_ref = db.collection(u'LinkinBioUsers')
    for i in range(0, len(feed_entries)):

        # Get post info dictionaries
        entry = feed_entries[i]
        user_ref = users_ref.document(entry['EntryOwnerId'])
        post_ref = user_ref.collection(u'portfolio').document(entry['EntryId'])
        user_info = user_ref.get().to_dict()
        basic_info = post_ref.get().to_dict()
        secret_info = post_ref.collection(u'secrets').document(u'secrets').get().to_dict()
        basic_info, secret_info = format_feed_data(basic_info, secret_info)

        # Merge
        feed_data.append(user_info | basic_info | secret_info)
    
    # Output to CSV
    feed_df = pd.DataFrame(feed_data, columns=FEED_COLUMN_NAMES)
    feed_df.to_csv(FilePaths.FEED_DATA_PATH, encoding='utf-8', index=False)

# Cleans up feed data and overwrite raw data csv
def fix_feed_data():
    feed_data = pd.read_csv(FilePaths.FEED_DATA_PATH)
    feed_data = convert_to_usable_date(feed_data)
    feed_data = standardized_business_names(feed_data)
    feed_data.to_csv(FilePaths.FEED_DATA_PATH, encoding='utf-8', index=False)
    feed_data.to_json(FilePaths.FEED_DATA_PATH_JSON, orient="records")

def convert_timestamp_to_date(timestamp):
    return datetime.utcfromtimestamp(timestamp).strftime('%Y-%m-%d')

def convert_deal_dates_to_date(long_date):
    try:
        return datetime.strptime(str(long_date), '%c').strftime('%Y-%m-%d')
    except:
        return long_date

def convert_to_usable_date(feed_data: pd.DataFrame):
    feed_data['TimePosted'] = feed_data['TimePosted_TIMESTAMP'].apply(convert_timestamp_to_date)
    feed_data['DealStarted'] = feed_data['DealStarted'].apply(convert_deal_dates_to_date)
    feed_data['DealEnded'] = feed_data['DealEnded'].apply(convert_deal_dates_to_date)
    return feed_data

# Standardized business names with similar spellings
def standardized_business_names(feed_data: pd.DataFrame):
    business_names_series = feed_data['BusinessName']
    business_names = business_names_series.unique()
    for name in business_names:
        closest_match = process.extractOne(name, COMMON_BUSINESS_NAMES)
        if closest_match[1] >= BUSINESS_NAME_BASELINE_SCORE:
            business_names_series = business_names_series.replace(name, closest_match[0])
    feed_data['BusinessName'] = business_names_series
    return feed_data


# Loads feed data for the rest of the script
def load_feed_data():
    global FEED_DATA_DF, FEED_DATA
    FEED_DATA_DF = pd.read_csv(FilePaths.FEED_DATA_PATH)
    FEED_DATA = FEED_DATA_DF.to_dict('records')


def generate_business_data():
    aggregate_by_business()

# Group by business
def aggregate_by_business():
    business_grouped = FEED_DATA_DF.groupby("BusinessName")

    # Calculate metric averages and counts
    business_agg = business_grouped.agg({'BrandReview': ['mean'], 'ProductQuality': ['mean'], 'UserName': ['count']})
    business_agg = business_agg.reset_index()
    business_agg.columns = ["BusinessName", "BrandReview", "ProductReview", "DealCount"]

    # Multi-grouping to find percent of recommendations
    recommendation_grouped = FEED_DATA_DF[FEED_DATA_DF['Recommendation'] == True].groupby(["BusinessName", "Recommendation"])
    recommendation_agg = recommendation_grouped.agg({'Recommendation': ['count']})
    recommendation_agg = recommendation_agg.reset_index()
    recommendation_agg.columns = ["BusinessName", "RecommendationGroup", "Recommendation_Yes"]
    business_agg['RecommendedPercentage'] = round(recommendation_agg['Recommendation_Yes'] / business_agg['DealCount'], 2)

    # Format column values
    business_agg['BrandReview'] = business_agg['BrandReview'].round(2)
    business_agg['ProductReview'] = business_agg['ProductReview'].round(2)
    business_agg.loc[:, 'RecommendedPercentage'] = business_agg['RecommendedPercentage'].map('{:.2%}'.format)
    
    # Save to CSV
    business_agg.to_csv(FilePaths.BUSINESS_DATA_PATH, encoding='utf-8', index=False)
    business_agg.to_json(FilePaths.BUSINESS_DATA_PATH_JSON, orient="records")


# General user details
def get_user_details(collection):
    return collection.document(u'UserDetails').get().to_dict()


# Count visits to wrkwme.io link visits
def get_linkinbio_visits_details(collection):
    return dict(LinkinBio_Visits_Count=len(collection.get()))


# Count visits to portfolio
def get_portfolio_visits_details(collection):
    return dict(Portfolio_Visits_Count=len(collection.get()))


# Users' social media details
def get_social_media_details(collection):
    sm_details = dict()
    for sm in collection.stream():
        details = sm.to_dict()
        sm_details[f"{sm.id}_Username"] = details['Username']
        if 'ProfileURL' in details:
            sm_details[f"{sm.id}_URL"] = details['ProfileURL']
    return sm_details


# LinkinBio links details
def get_linkinbio_links_details(collection):
    link_count = 0
    total_linkinbio_to_link_click_conversions = 0

    for link_doc in collection.stream():
        link_count += 1
        for link_collection in link_doc.reference.collections():
            if link_collection.id == "analytics":
                total_linkinbio_to_link_click_conversions += len(link_collection.get())
    return dict(
        LinkinBio_Links_Count = link_count,
        Total_LinkinBio_Link_Click_Conversions = total_linkinbio_to_link_click_conversions
    )


# Retrieves all LinkinBio users and their information
def get_user_information():

    user_data = []
    users_ref = db.collection(u'LinkinBioUsers')

    # Iterate each LinkinBio user
    for user_doc in users_ref.stream():

        basic_details = dict()
        user_details = dict()
        linkinbio_visits_details = dict()
        portfolio_visits_details = dict()
        social_media_details = dict()
        linkinbio_links_details = dict()

        # Get basic user details
        basic_details = user_doc.to_dict()

        # Iterate subcollections and get details
        subcollections = user_doc.reference.collections()
        for collection in subcollections:
            if collection.id == "UserDetails":
                user_details = get_user_details(collection)
            if collection.id == "LinkPageAnalytics":
                linkinbio_visits_details = get_linkinbio_visits_details(collection)
            if collection.id == "PortfolioPageAnalytics":
                portfolio_visits_details = get_portfolio_visits_details(collection)
            if collection.id == "SocialMedia":
                social_media_details = get_social_media_details(collection)
            if collection.id == "links":
                linkinbio_links_details = get_linkinbio_links_details(collection)
                
        # Append user info before going to the next user
        user_data.append(basic_details | user_details | linkinbio_visits_details | portfolio_visits_details | social_media_details | linkinbio_links_details)

    # Output to CSV
    user_data_df = pd.DataFrame(user_data, columns=USER_COLUMN_NAMES)
    user_data_df.to_csv(FilePaths.USER_DATA_PATH, encoding='utf-8', index=False)


# Loads user data
def load_user_information():
    global USER_DATA
    user_data_df = pd.read_csv(FilePaths.USER_DATA_PATH)
    USER_DATA = user_data_df.to_dict('records')


# Top level data summarizations
def get_key_metrics():
    
    KEY_METRICS['total_user_count'] = len(USER_DATA)
    KEY_METRICS['total_deal_count'] = len(FEED_DATA)
    KEY_METRICS['total_brand_count'] = len(FEED_DATA_DF["BusinessName"].unique())

    # Save to json file
    with open("./data/json/key_metrics.json", "w") as outfile:
        json.dump(KEY_METRICS, outfile)


# Verifies prerequisites
def verify_prerequisites():
    if not os.path.exists("./data"):
        os.makedirs("./data")
    if not os.path.exists("./data/business_data"):
        os.makedirs("./data/business_data")


# Testing what score passes for business comparison
def business_name_comparison_test():
    cont = "y"
    while cont == "y":
        name1 = input("1: ")
        name2 = input("2: ")
        print(fuzz.WRatio(name1, name2))
        cont = input("Continue? (y/n): ")


if __name__ == "__main__":
    # Make sure file paths exist
    verify_prerequisites()

    # Feed data
    # get_feed_data()
    fix_feed_data()
    load_feed_data()

    # User data
    # get_user_information()
    load_user_information()

    # Business summaries
    generate_business_data()

    # Top level metrics
    get_key_metrics()
