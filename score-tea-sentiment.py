import nltk
import pandas as pd

from nltk.tokenize import word_tokenize
from nltk.sentiment import SentimentIntensityAnalyzer
from config import FilePaths

def filter_tokens(token_list):
    return [w for w in token_list if w.isalpha()]

def score_sentiment():

    feed_data = pd.read_csv(FilePaths.FEED_DATA_PATH)
    tea = feed_data['Tea']

    sia = SentimentIntensityAnalyzer()
    sentiment = tea.apply(lambda x: sia.polarity_scores(str(x)))
    output = pd.DataFrame(sentiment.tolist())
    output['Tea'] = tea

    output.to_csv("./data/tea_sentiment/tea_sentiment_scores.csv", encoding='utf-8', index=False)
    return

    # Load tea
    feed_data = pd.read_csv(FEED_DATA_PATH)
    tea =  feed_data['Tea'].apply(lambda x: word_tokenize(str(x)))
    tea = tea.apply(filter_tokens)

    # Remove stop words from tea
    stopwords = nltk.corpus.stopwords.words("english")
    tea = [w for w in tea if w.lower() not in stopwords]


def download_nltk():
    nltk.download([
        "names",
        "stopwords",
        "state_union",
        "twitter_samples",
        "movie_reviews",
        "averaged_perceptron_tagger",
        "vader_lexicon",
        "punkt",
    ])

if __name__ == "__main__":
    # download_nltk()
    score_sentiment()
    exit()