import pandas as pd
import firebase_admin
from firebase_admin import credentials
from firebase_admin import firestore
from config import FIREBASE_KEY_PATH, FEED_DATA_PATH, USER_DATA_PATH

cred = credentials.Certificate(FIREBASE_KEY_PATH)
firebase_admin.initialize_app(cred)
db = firestore.client()

FEED_COLUMN_NAMES = ['TimePosted_TIMESTAMP', 'UserName', 'TimePosted', 'TimeUpdated', 'DealStarted', 'DealEnded', 'BusinessName', 'BusinessCategory',
                'ProductPromoted', 'PlatformPosted', 'BrandReview', 'ProductQuality', 'Recommendation', 'PaymentType']
USER_COLUMN_NAMES = ['UserName', 'Firstname', 'Lastname', 'Title', 'State', 'City', 'Gender', 'Category', 'Ethnicity', 'PhoneNumber', 'Email',
                    'Instagram_Username', 'Instagram_URL', 'TikTok_Username', 'TikTok_URL', 'Twitter_Username', 'Twitter_URL',
                    'LinkinBio_Visits_Count', 'Portfolio_Visits_Count', 'LinkinBio_Links_Count', 'Total_LinkinBio_Link_Click_Conversions']
PAYMENT_TYPES = ['None', 'Paid Partnership', 'Product Gifting', 'Both']
PLATFORM_TYPES = ['None', 'Instagram', 'Twitter', 'TikTok', 'Youtube']

FEED_DATA = []
USER_DATA = []


# Returns list with objects of keys EntryId, EntryOwnerId, and TimePosted
def get_all_feed_entries() -> list[dict]:
    feed_ref = db.collection(u'Feed')
    feed_docs = feed_ref.stream()
    for doc in feed_docs:
        FEED_DATA.append(doc.to_dict())


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

    # Get feed entries
    get_all_feed_entries()

    # Iterate feed entries
    users_ref = db.collection(u'LinkinBioUsers')
    for i in range(0, len(FEED_DATA)):

        # Get post info dictionaries
        entry = FEED_DATA[i]
        user_ref = users_ref.document(entry['EntryOwnerId'])
        post_ref = user_ref.collection(u'portfolio').document(entry['EntryId'])
        user_info = user_ref.get().to_dict()
        basic_info = post_ref.get().to_dict()
        secret_info = post_ref.collection(u'secrets').document(u'secrets').get().to_dict()
        basic_info, secret_info = format_feed_data(basic_info, secret_info)

        # Merge
        FEED_DATA[i] = user_info | basic_info | secret_info
    
    # Output to CSV
    feed_data_df = pd.DataFrame(FEED_DATA, columns=FEED_COLUMN_NAMES)
    feed_data_df.to_csv(FEED_DATA_PATH, encoding='utf-8', index=False)


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
        USER_DATA.append(basic_details | user_details | linkinbio_visits_details | portfolio_visits_details | social_media_details | linkinbio_links_details)

    # Output to CSV
    user_data_df = pd.DataFrame(USER_DATA, columns=USER_COLUMN_NAMES)
    user_data_df.to_csv(USER_DATA_PATH, encoding='utf-8', index=False)

        

if __name__ == "__main__":
    get_feed_data()
    get_user_information()