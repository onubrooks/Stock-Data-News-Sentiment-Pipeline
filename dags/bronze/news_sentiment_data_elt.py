import nltk
import pandas as pd
from nltk.corpus import stopwords
from nltk.stem import PorterStemmer
from sklearn.decomposition import LatentDirichletAllocation
from sklearn.feature_extraction.text import CountVectorizer, TfidfTransformer
from textblob import TextBlob

nltk.download('stopwords')


def preprocess_text(text):
    # Remove stop words and punctuation
    if not isinstance(text, str):
        text = str(text)
    text = ' '.join(
        [
            word
            for word in text.split()
            if word not in stopwords.words('english')
        ]
    )
    text = ''.join(c for c in text if c.isalnum() or c == ' ')
    # Apply stemming
    stemmer = PorterStemmer()
    text = ' '.join([stemmer.stem(word) for word in text.split()])
    return text


def analyze_sentiment(text):
    analysis = TextBlob(text)
    return analysis.sentiment.polarity


def extract_features(df):
    df['combined_text'] = (
        df['title']
        + '. '
        + df['url']
        + '. '
        + df['summary']
        + '. '
        + df['main_topic']
    )
    df.drop(
        ['banner_image', 'source', 'source_domain', 'main_topic'],
        axis=1,
        inplace=True,
    )
    df['cleaned_summary'] = df['combined_text'].apply(preprocess_text)
    df['sentiment_score'] = df['cleaned_summary'].apply(analyze_sentiment)
    df['word_count'] = df['cleaned_summary'].apply(len)
    df['char_count'] = df['cleaned_summary'].str.len()
    # Create a document-term matrix
    vectorizer = CountVectorizer()
    X = vectorizer.fit_transform(df['cleaned_summary'])

    # Apply TF-IDF transformation
    tfidf_transformer = TfidfTransformer()
    X_tfidf = tfidf_transformer.fit_transform(X)

    # Apply LDA
    lda_model = LatentDirichletAllocation(n_components=10, random_state=42)
    lda_model.fit(X_tfidf)

    # Get topic probabilities for each document
    topic_probabilities = lda_model.transform(X_tfidf)

    # Add topic probabilities to the DataFrame
    df['topic_probabilities'] = topic_probabilities.tolist()
    return df


def extract_features_from_news_data(ti):
    news_data = ti.xcom_pull(task_ids='get_sentiment_data_s3')

    # Apply preprocessing and feature extraction
    news_data = extract_features(news_data)
    return news_data
