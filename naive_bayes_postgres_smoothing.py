import psycopg2

conn = psycopg2.connect(dbname='naive', user='postgres', host='/tmp')
c = conn.cursor()


# Create test sets (90% train, 10% test)
print "Creating testurls..."
c.execute(
    '''CREATE MATERIALIZED VIEW testurls AS
    SELECT * FROM urls
    ORDER BY RANDOM()
    LIMIT (SELECT COUNT(*) FROM urls)/10;
    ''')

print "Creating trainurls..."
c.execute(
    '''CREATE MATERIALIZED VIEW trainurls AS
    SELECT * FROM urls WHERE NOT EXISTS
    (SELECT 1 FROM testurls WHERE testurls.url = urls.url);
    ''')

print "Creating bag..."
c.execute(
    '''CREATE MATERIALIZED VIEW bag AS
    SELECT url_id, url, label, word_id, word, COUNT(1) AS cnt
    FROM wordlocation
    JOIN urls
    ON urls.id=wordlocation.url_id
    JOIN wordlist
    ON wordlist.id=wordlocation.word_id
    GROUP BY wordlocation.url_id, wordlocation.word_id, urls.url, urls.label, wordlist.word;
    ''')

print "Creating testbag..."
c.execute(
    '''CREATE MATERIALIZED VIEW testbag AS
    SELECT url_id, bag.url AS url, bag.label AS label, word_id, word, cnt
    FROM bag
    JOIN testurls
    ON testurls.url=bag.url;
    ''')

print "Creating trainbag..."
c.execute(
    '''CREATE MATERIALIZED VIEW trainbag AS
    SELECT url_id, bag.url AS url, bag.label AS label, word_id, word, cnt
    FROM bag
    JOIN trainurls
    ON trainurls.url=bag.url;
    ''')

# Create Priors
print "Creating priors..."
c.execute(
    '''CREATE MATERIALIZED VIEW priors AS
    SELECT
        label,
        CAST(COUNT(1) AS REAL) / (SELECT COUNT(1) FROM urls) AS prob
    FROM urls
    GROUP BY label;
    ''')

# Create Conditional Probability Table
print "Creating cpt..."
c.execute(
    '''CREATE MATERIALIZED VIEW cpt AS
    SELECT
        word_id,
        trainbag.label,
        (CAST(sum(trainbag.cnt) AS REAL) + 1) / (totals.cnt) AS prob
    FROM trainbag
    JOIN
    (SELECT label, sum(cnt) + (SELECT COUNT(*) FROM wordlist) AS cnt FROM bag GROUP BY label) totals
    ON totals.label=trainbag.label
    GROUP BY trainbag.label, word_id, totals.cnt;
    ''')

print "Creating predict..."
c.execute(
    '''CREATE MATERIALIZED VIEW predict AS
    SELECT
        a.url_id,
        a.label,
        LOG(a.p_prob) + SUM(LOG(a.cpt_prob)) AS prob
    FROM
      (SELECT
            testbag.word_id,
            testbag.url_id,
            priors.label,
            priors.prob AS p_prob,
            COALESCE(cpt.prob, 1.0 / (SELECT COUNT(*) FROM wordlist)) AS cpt_prob
        FROM testbag CROSS JOIN priors
        LEFT OUTER JOIN cpt
        ON
            testbag.word_id=cpt.word_id AND
            cpt.label=priors.label) a
    GROUP BY
        a.url_id, a.label, a.p_prob;
    ''')

print "Creating results..."
c.execute(
    '''CREATE MATERIALIZED VIEW results AS
    SELECT
        urls.id AS url_id,
        urls.label AS label,
        predict.label AS prediction,
        predict.prob AS prob
    FROM urls
    JOIN predict
    ON urls.id=predict.url_id
    JOIN (SELECT url_id, max(prob) AS prob FROM predict GROUP BY url_id) m
    ON predict.prob=m.prob;
    ''')

print "Committing..."
conn.commit()


# Get accuracy
c.execute(
'''SELECT COUNT(1) FROM results
WHERE results.label=results.prediction;
''')
correct = int(c.fetchone()[0])
c.execute(
'''SELECT COUNT(1) FROM results
WHERE results.label!=results.prediction;
''')
incorrect = int(c.fetchone()[0])

c.close()

print "correct:", correct
print "incorrect:", incorrect
print "accuracy:", float(correct) / (correct + incorrect)

