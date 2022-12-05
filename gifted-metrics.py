import pandas as pd
import json
import firebase_admin
from firebase_admin import credentials
from firebase_admin import firestore
from config import FIREBASE_KEY_PATH

cred = credentials.Certificate(FIREBASE_KEY_PATH)
firebase_admin.initialize_app(cred)
db = firestore.client()

FEED_COLUMN_NAMES = ['TimePosted_TIMESTAMP', 'UserName', 'TimePosted', 'TimeUpdated', 'DealStarted', 'DealEnded', 'BusinessName', 'BusinessCategory',
                'ProductPromoted', 'PlatformPosted', 'BrandReview', 'ProductQuality', 'Recommendation', 'PaymentType']
USER_COLUMN_NAMES = ['UserName', 'Firstname', 'Lastname', 'Title', 'State', 'City', 'Gender', 'Category', 'Ethnicity', 'PhoneNumber', 'Email',
                    'Instagram_Username', 'Instagram_URL', 'TikTok_Username', 'TikTok_URL', 'Twitter_Username', 'Twitter_URL']
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
    get_all_feed_entries()

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
    feed_data_df.to_csv("C:/Users/Zachary/Desktop/feed_data.csv", encoding='utf-8', index=False)


def get_user_information():
    users_ref = db.collection(u'LinkinBioUsers')
    users_docs = users_ref.stream()


    for doc in users_docs:
        basic_details = doc.to_dict()
        user_details = doc.reference.collection(u'UserDetails').document(u'UserDetails').get().to_dict()
        
        social_medias = doc.reference.collection(u'SocialMedia').stream()
        sm_details = dict()
        for sm in social_medias:
            details = sm.to_dict()
            sm_details[f"{sm.id}_Username"] = details['Username']
            if 'ProfileURL' in details:
                sm_details[f"{sm.id}_URL"] = details['ProfileURL']

        USER_DATA.append(basic_details | user_details | sm_details)

    # Output to CSV
    user_data_df = pd.DataFrame(USER_DATA, columns=USER_COLUMN_NAMES)
    user_data_df.to_csv("C:/Users/Zachary/Desktop/user_data.csv", encoding='utf-8', index=False)

        



if __name__ == "__main__":
    # get_feed_data()
    get_user_information()