SQLNaiveBayes
=============

Naive Bayes algorithm implemented using SQL. The data naive_melody.sql contains a large amount of data from New York Times web scraping and the goal is to predict which category each scraped website was in. THis was done purely using the words from the body of the website. The data is moved immediately in raw JSON form into mongo DB and then just the body text is extracted out with the category (training label) into postgres SQL where all the real work is done. The data is cleaned up and split apart in python (mongo_to_postgres.py) and then moved over to postgres.
