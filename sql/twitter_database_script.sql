/* CREATE TABLES AUTHOR, TWEET AND TOKENS */

CREATE TABLE IF NOT EXISTS authors (
    name VARCHAR NOT NULL
);

CREATE TABLE IF NOT EXISTS tweets (
    tweet_text TEXT,
    tweet_date DATETIME,
    nb_words INT,
    author_id INT,
    FOREIGN KEY (author_id) REFERENCES author(id)
);

CREATE TABLE IF NOT EXISTS tokens (
    word TEXT
);

CREATE TABLE IF NOT EXISTS tokens_by_article (
    tweet_id INT, 
    words_id INT,
    FOREIGN KEY (tweet_id) REFERENCES tweets(rowid),
    FOREIGN KEY (words_id) REFERENCES words(rowid)
);