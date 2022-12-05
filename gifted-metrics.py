import pandas as pd
import firebase_admin
from firebase_admin import credentials
from firebase_admin import firestore
from config import FIREBASE_KEY_PATH

cred = credentials.Certificate(FIREBASE_KEY_PATH)
firebase_admin.initialize_app(cred)
db = firestore.client()

# for doc in docs:
#     print(f'{doc.id} => {doc.to_dict()}')


def get_feed_metrics():
    feed_doc_metrics()

# Returns pandas array with EntryId, EntryOwnerId, and TimePosted
def feed_doc_metrics():
    feed_ref = db.collection(u'Feed')
    feed_docs = feed_ref.stream()

    feed_dicts = []
    for doc in feed_docs:
        feed_dicts.append(doc.to_dict())

    feed_posts_df = pd.DataFrame(feed_dicts, columns=["EntryId", "EntryOwnerId", "TimePosted"])
    print(feed_posts_df)





if __name__ == "__main__":
    get_feed_metrics()