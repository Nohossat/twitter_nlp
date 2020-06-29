# NLP with Twitter API
 
## Fetch 1000 tweets with Twython

We use th Twython module to easily fetch tweets.

## NLP Feature engineering : Tokenization

The application tokenizes the tweets fetched by the Twitter API and stores the values in local MongoDB / SQLite databases. It does as follow :

- Count words
- Tokenization
- Store data in a SQLite and MongoDB Databases with the following info :
    - author
    - date
    - tweet
    - number of words
    - words without stopwords

## How to launch the app

You must install **MongoDB** and **SQLite** in your machine before using the app.

### 1. Download the GitHub repository

```python
git clone https://github.com/Nohossat/twitter_nlp.git
```

### 2. Install dependencies

```python
python -m venv venv/
pip install -r requirements.txt
```

### 3. Launch the app

You can launch the app from the CLI with 2 arguments : the **JSON credentials file path** and the **search value**.

```python
python app.py --credentials credentials.json --search PyCon
```

A template of the credentials file is provided (**credentials.json**) but you have to replace the missing credentials with your own.

