from pymongo import MongoClient
import psycopg2
from nltk.tokenize import word_tokenize

client = MongoClient()
coll = client.nyt_dump.articles

conn = psycopg2.connect(dbname='articles', user='postgres', host='/tmp')
cur = conn.cursor()


cur.execute(
    '''CREATE TABLE labels (
        id integer PRIMARY KEY,
        name text);
    ''')

cur.execute(
    '''CREATE TABLE documents (
        id integer PRIMARY KEY,
        url text,
        label_id integer);
    ''')

cur.execute(
    '''CREATE TABLE words (
        id integer PRIMARY KEY,
        word text);
    ''')

cur.execute(
    '''CREATE TABLE document_words (
        id integer PRIMARY KEY,
        word_id integer,
        document_id integer);
    ''')

labels = {}
words = {}

doc_id = 0
doc_words_id = 0

for article in coll.find():
    label = article['section_name']
    if label not in labels:
        label_id = len(labels)
        labels[label] = label_id
        cur.execute('''INSERT INTO labels VALUES (%d, '%s');''' % (label_id, label))
    else:
        label_id = labels[label]

    cur.execute('''INSERT INTO documents VALUES (%d, '%s', %d);''' % (doc_id, article['web_url'], label_id))

    content = ' '.join(article['content']).lower()
    for word in word_tokenize(content):
        word = word.replace("'", "''")
        if word not in words:
            word_id = len(words)
            words[word] = word_id
            cur.execute('''INSERT INTO words VALUES (%d, '%s');''' % (word_id, word))
        else:
            word_id = words[word]
        cur.execute('''INSERT INTO document_words VALUES (%d, %d, %d);''' % (doc_words_id, word_id, doc_id))
        doc_words_id += 1
    doc_id += 1

print "num labels:", len(labels)
print "num words:", len(words)
print "num documents:", doc_id
print "len of document_words:", doc_words_id

conn.commit()
conn.close()
