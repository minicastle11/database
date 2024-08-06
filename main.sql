SELECT fre.docId, rcmdDocId, sim.score, post_title, post_date FROM year_pub_count_table AS year
    JOIN db_proj_datasrc.frequency AS fre ON year.post_title =  fre.docTitle
JOIN db_proj_datasrc.similarity AS sim ON fre.docId = sim.docId
GROUP BY fre.docId, rcmdDocId, sim.score, post_title, post_date ORDER BY score desc ;

-- Step 1: Find the most recently created document
WITH recent_doc AS (
    SELECT docId, post_title, post_date
    FROM db_proj_datasrc.frequency
    ORDER BY post_date DESC
    LIMIT 1
)

-- Step 2: Find the top five most similar documents to the most recent document, excluding the document itself
SELECT sim.rcmdDocId, sim.score, post_title, post_date
FROM db_proj_datasrc.similarity AS sim
JOIN db_proj_datasrc.frequency AS fre ON sim.rcmdDocId = fre.docId
WHERE sim.docId = (SELECT docId FROM recent_doc)
AND sim.rcmdDocId != (SELECT docId FROM recent_doc)
ORDER BY sim.score DESC
LIMIT 5;


-- Step 1: Find the most recently created document
WITH recent_doc AS (
    SELECT fre.docId, year.post_title, year.post_date
    FROM year_pub_count_table AS year
    JOIN db_proj_datasrc.frequency AS fre ON year.post_title = fre.docTitle
    ORDER BY year.post_date DESC
    LIMIT 1
)

-- Step 2: Find the top five most similar documents to the most recent document, excluding the document itself
SELECT sim.rcmdDocId, sim.score, year.post_title, year.post_date
FROM db_proj_datasrc.similarity AS sim
JOIN db_proj_datasrc.frequency AS fre ON sim.rcmdDocId = fre.docId
JOIN year_pub_count_table AS year ON fre.docTitle = year.post_title
WHERE sim.docId = (SELECT docId FROM recent_doc)
AND sim.rcmdDocId != (SELECT docId FROM recent_doc)
ORDER BY sim.score DESC
LIMIT 5;


WITH word_frequencies AS (
    SELECT tfidfWord, COUNT(*) AS frequency
    FROM db_proj_datasrc.frequency
    GROUP BY tfidfWord
    ORDER BY frequency DESC
    LIMIT 1 OFFSET 7  -- get the 8th most frequent word
),
word_docs AS (
    SELECT docId, score AS score, docTitle
    FROM db_proj_datasrc.frequency
    WHERE tfidfWord = (SELECT tfidfWord FROM word_frequencies)
    ORDER BY score DESC
    LIMIT 1 OFFSET 149  -- get the document with the 150th highest score
),
fourth_similar_doc AS (
    SELECT sim.rcmdDocId
    FROM db_proj_datasrc.similarity AS sim
    WHERE sim.docId = (SELECT docId FROM word_docs)
    ORDER BY sim.score DESC
    LIMIT 1 OFFSET 3  -- get the document with the 4th highest similarity
)
SELECT doc._id, doc.docTitle, doc.author_name
FROM db_proj_24.documents AS doc
WHERE doc._id = (SELECT rcmdDocId FROM fourth_similar_doc);

WITH ranked_frequencies AS (
    SELECT
        tfidfWord,
        COUNT(tfidfWord) AS frequency,
        DENSE_RANK() OVER (ORDER BY COUNT(tfidfWord) DESC))
    FROM db_
    GROUP BY tfidfWord
)SELECT tfidfWord, frequency
FROM ranked_frequencies
WHERE rank = 8;

SELECT docId, score, docTitle
    FROM db_proj_datasrc.frequency
    WHERE tfidfWord = (SELECT tfidfWord FROM word_frequencies)
    ORDER BY score DESC
    LIMIT 1 OFFSET 149;

WITH word_docs AS (
    SELECT docId, score, docTitle
    FROM db_proj_datasrc.frequency
    WHERE tfidfWord = (SELECT tfidfWord FROM word_frequencies)
    ORDER BY score DESC
    LIMIT 1 OFFSET 149  -- get the document with the 150th highest score
),
fourth_similar_doc AS (
    SELECT sim.rcmdDocId, sim.score , word_docs.docTitle
    FROM db_proj_datasrc.similarity AS sim
    JOIN word_docs ON sim.docId = word_docs.docId
    ORDER BY sim.score DESC
    LIMIT 1 OFFSET 3  -- get the document with the 4th highest similarity
)
SELECT count._id, fourth_similar_doc.docTitle, count.name
FROM db_proj_24.user_count_table AS count
JOIN fourth_similar_doc ON count._id = fourth_similar_doc.rcmdDocId;



WITH ranked_frequencies AS (
    SELECT tfidfWord,
           COUNT(tfidfWord) AS frequency,
           DENSE_RANK() over (ORDER BY COUNT(tfidfWord)) AS nrank
    FROM db_proj_datasrc.frequency
    GROUP BY tfidfWord
),rank_first AS (SELECT tfidfWord, frequency
FROM ranked_frequencies
WHERE nrank = 8),

;

CREATE INDEX idx_frequency_docId ON db_proj_24.frequency(docId, docTitle);

CREATE INDEX idx_frequency_docId ON db_proj_24.frequency(docId, docTitle);
WITH ranked_frequencies AS (
    SELECT
        tfidfWord,
        COUNT(tfidfWord) AS frequency,
        DENSE_RANK() OVER (ORDER BY COUNT(tfidfWord) DESC) AS rank  -- 별칭을 부여
    FROM db_proj_datasrc.frequency
    GROUP BY tfidfWord
)
SELECT tfidfWord, frequency
FROM ranked_frequencies
WHERE rank = 8;



WITH word_counts AS (
    SELECT
        tfidfWord,
        COUNT(tfidfWord) AS frequency
    FROM db_proj_datasrc.frequency
    GROUP BY tfidfWord
),
ranked_frequencies AS (
    SELECT
        tfidfWord,
        frequency,
        DENSE_RANK() OVER (ORDER BY frequency DESC) AS rank  -- 별칭을 부여
    FROM word_counts
)
SELECT tfidfWord, frequency
FROM ranked_frequencies
WHERE rank = 8;

WITH ranked_frequencies AS (
    SELECT tfidfWord,
           COUNT(tfidfWord) AS frequency,
           DENSE_RANK() OVER (ORDER BY COUNT(tfidfWord) DESC) AS nrank
    FROM db_proj_datasrc.frequency
    GROUP BY tfidfWord
),
rank_first AS (
    SELECT tfidfWord, frequency
    FROM ranked_frequencies
    WHERE nrank = 8
),
word_docs AS (
    SELECT docId, score AS score, docTitle
    FROM db_proj_datasrc.frequency
    WHERE tfidfWord IN (SELECT tfidfWord FROM rank_first)  -- 8번째로 빈도가 높은 단어들 중 하나를 선택
    ORDER BY score DESC
    LIMIT 1 OFFSET 149  -- 점수가 150번째로 높은 문서 선택
),
fourth_similar_doc AS (
    SELECT sim.rcmdDocId, sim.score AS score
    FROM db_proj_datasrc.similarity AS sim
    WHERE sim.docId = (SELECT docId FROM word_docs)
    ORDER BY sim.score DESC
    LIMIT 1 OFFSET 3  -- 4번째로 유사한 문서 선택
)
SELECT fre.docId, doc.post_title, doc.post_writer
FROM db_proj_24.year_pub_count_table AS doc JOIN db_proj_datasrc.frequency AS fre ON docTitle = post_title
WHERE fre.docId = (SELECT rcmdDocId FROM fourth_similar_doc)
GROUP BY doc.post_title, fre.docId, doc.post_writer;



# 7번
WITH ranked_frequencies AS (
    SELECT tfidfWord,
           COUNT(tfidfWord) AS frequency,
           DENSE_RANK() OVER (ORDER BY COUNT(tfidfWord) DESC) AS nrank
    FROM db_proj_datasrc.frequency
    GROUP BY tfidfWord
),
rank_first AS (
    SELECT tfidfWord, frequency
    FROM ranked_frequencies
    WHERE nrank = 8
),
word_docs AS (
    SELECT docId, score AS score, docTitle
    FROM db_proj_datasrc.frequency
    WHERE tfidfWord IN (SELECT tfidfWord FROM rank_first)  -- 8번째로 빈도가 높은 단어들 중 하나를 선택
    ORDER BY score DESC
    LIMIT 1 OFFSET 149  -- 점수가 150번째로 높은 문서 선택
),
fourth_similar_doc AS (
    SELECT sim.rcmdDocId, sim.score AS score
    FROM db_proj_datasrc.similarity AS sim
    WHERE sim.docId = (SELECT docId FROM word_docs)
    ORDER BY sim.score DESC
    LIMIT 1 OFFSET 3  -- 4번째로 유사한 문서 선택
)
SELECT fre.docId, doc.post_title, doc.post_writer
FROM db_proj_24.year_pub_count_table AS doc JOIN db_proj_datasrc.frequency AS fre ON docTitle = post_title
WHERE fre.docId = (SELECT rcmdDocId FROM fourth_similar_doc)
GROUP BY doc.post_title, fre.docId, doc.post_writer;



SELECT f.docId, f.docTitle, a.post_writer
FROM db_proj_24.year_pub_count_table a
JOIN frequency f ON a.post_title = f.docTitle
JOIN db_proj_24.doc_info_table1  d ON a.post_title = d.doc_title
WHERE d.first_char_title LIKE 'ㅁ'
  AND f.tfidfWord = '고찰'
ORDER BY f.score DESC
LIMIT 1;




WITH tfidf_ranked AS (
    SELECT
        f.docId,
        d.post_body,
        LENGTH(d.post_body) AS post_body_length,
        f.tfidfWord,
        f.score,
        ROW_NUMBER() OVER (PARTITION BY f.docId ORDER BY f.score DESC) AS rn,
        LAG(f.score) OVER (PARTITION BY f.docId ORDER BY f.score DESC) AS prev_tfidfScore
    FROM
        db_proj_datasrc.frequency AS f
        JOIN db_proj_24.year_pub_count_table AS d ON f.docTitle = d.post_title
    WHERE
        LENGTH(d.post_body) > 150
),
predominance_calc AS (
    SELECT
        docId,
        post_body,
        post_body_length,
        tfidfWord,
        score,
        (score - prev_tfidfScore) AS predominance
    FROM
        tfidf_ranked
    WHERE
        prev_tfidfScore IS NOT NULL
),
top_docs AS (
    SELECT
        docId,
        post_body,
        post_body_length,
        tfidfWord,
        score,
        predominance,
        ROW_NUMBER() OVER (ORDER BY predominance DESC) AS overall_rank
    FROM
        predominance_calc
)
SELECT
    docId,
    post_body_length,
    tfidfWord,
    score,
    predominance
FROM
    top_docs
WHERE
    overall_rank <= 10;



WITH ranked_frequencies AS (
    SELECT
        tfidfWord,
        COUNT(tfidfWord) AS frequency,
        DENSE_RANK() OVER (ORDER BY COUNT(tfidfWord) DESC) AS nrank
    FROM
        db_proj_24.frequency
    GROUP BY
        tfidfWord
),
rank_first AS (
    SELECT
        tfidfWord
    FROM
        ranked_frequencies
    WHERE
        nrank = 8
),
word_docs AS (
    SELECT
        docId,
        score,
        docTitle,
        ROW_NUMBER() OVER (ORDER BY score DESC) AS rnum
    FROM
        db_proj_24.frequency
    WHERE
        tfidfWord IN (SELECT tfidfWord FROM rank_first)
),
target_doc AS (
    SELECT
        docId
    FROM
        word_docs
    WHERE
        rnum = 150
),
fourth_similar_doc AS (
    SELECT
        rcmdDocId,
        ROW_NUMBER() OVER (ORDER BY score DESC) AS rnum
    FROM
        db_proj_24.similarity
    WHERE
        docId = (SELECT docId FROM target_doc)
),
final_doc AS (
    SELECT
        rcmdDocId
    FROM
        fourth_similar_doc
    WHERE
        rnum = 4
)
SELECT
    f.docId,
    y.post_title,
    y.post_writer
FROM
    db_proj_24.year_pub_count_table y
JOIN
    db_proj_24.frequency f ON y.post_title = f.docTitle
WHERE
    f.docId = (SELECT rcmdDocId FROM final_doc)
GROUP BY
    f.docId, y.post_title, y.post_writer;


SELECT MONTH(post_date) AS month, COUNT(*) AS publication_count
FROM db_proj_24.year_pub_count_table
GROUP BY MONTH(post_date)
ORDER BY publication_count DESC
LIMIT 1;
SELECT MONTH(allrecords.post_date) AS month, COUNT(*) AS publication_count
FROM db_proj_datasrc.allrecords
GROUP BY MONTH(allrecords.post_date)
ORDER BY publication_count DESC
LIMIT 1;

select allrecords.top_category, count(*) from db_proj_datasrc.allrecords
GROUP BY allrecords.top_category;


# 3번
SELECT YEAR(allrecords.post_date) AS year, COUNT(*) FROM db_proj_datasrc.allrecords
WHERE allrecords.post_body LIKE '%North Korea%' AND allrecords.post_body LIKE '%corona%'
GROUP BY year
ORDER BY year DESC;


#4번
WITH sorted_similarity AS (
    SELECT docID, score
    FROM similarity
    WHERE score != '1'
    ORDER BY score DESC
),
unique_frequency AS (
    SELECT docID, MIN(docTitle) as docTitle
    FROM frequency
    GROUP BY docID
)
SELECT ar.*
FROM sorted_similarity s
JOIN unique_frequency f ON s.docID = f.docID
JOIN db_proj_datasrc.allrecords ar ON ar.doc_title = f.docTitle
WHERE ar.email LIKE '****@handong.edu' OR ar.email LIKE '****@handong.ac.kr';


WITH most_recent_doc AS (
    SELECT f.docId
    FROM year_pub_count_table y
    JOIN db_proj_24.frequency f ON y.post_title = f.docTitle
    ORDER BY y.post_date DESC
    LIMIT 1
),
similar_docs AS (
    SELECT s.rcmdDocId, s.score
    FROM db_proj_24.similarity s
    JOIN most_recent_doc mrd ON s.docId = mrd.docId
    WHERE s.rcmdDocId != mrd.docId
    ORDER BY s.score DESC
    LIMIT 5
)
SELECT f.docId,s.rcmdDocId, s.score, y.post_title, y.post_date
FROM similar_docs s
JOIN db_proj_24.frequency f ON s.rcmdDocId = f.docId
JOIN year_pub_count_table y ON f.docTitle = y.post_title
GROUP BY f.docId,s.rcmdDocId, s.score, y.post_title, y.post_date
ORDER BY s.score DESC;



SELECT fre.tfidfWord, COUNT(fre.tfidfWord) AS total_tfidf
FROM db_proj_24.frequency AS fre
JOIN db_proj_24.doc_info_table2 AS link ON fre.docTitle = link.doc_title
JOIN db_proj_24.doc_info_table1 AS first ON link.doc_title = first.doc_title
WHERE (link.docURL LIKE '%.org%' OR link.docURL LIKE '%.or.kr%')
    AND (SUBSTRING(first.first_char_title, 1, 1) BETWEEN 'ㄱ' AND 'ㅎ')
    AND fre.score > 0
GROUP BY fre.tfidfWord
ORDER BY total_tfidf DESC;

WITH ranked_frequencies AS (
    SELECT
        tfidfWord,
        COUNT(tfidfWord) AS frequency,
        DENSE_RANK() OVER (ORDER BY COUNT(tfidfWord) DESC) AS nrank
    FROM
        db_proj_24.frequency
    GROUP BY
        tfidfWord
),
rank_first AS (
    SELECT
        tfidfWord
    FROM
        ranked_frequencies
    WHERE
        nrank = 8
),
word_docs AS (
    SELECT
        docId,
        score,
        docTitle,
        ROW_NUMBER() OVER (ORDER BY score DESC) AS rnum
    FROM
        db_proj_24.frequency
    WHERE
        tfidfWord IN (SELECT tfidfWord FROM rank_first)
),
target_doc AS (
    SELECT
        docId
    FROM
        word_docs
    WHERE
        rnum = 150
),
fourth_similar_doc AS (
    SELECT
        rcmdDocId,
        ROW_NUMBER() OVER (ORDER BY score DESC) AS rnum
    FROM
        db_proj_24.similarity
    WHERE
        docId = (SELECT docId FROM target_doc)
),
final_doc AS (
    SELECT
        rcmdDocId
    FROM
        fourth_similar_doc
    WHERE
        rnum = 4
)
SELECT
    f.docId,
    y.post_title,
    y.post_writer
FROM
    db_proj_24.year_pub_count_table y
JOIN
    db_proj_24.frequency f ON y.post_title = f.docTitle
WHERE
    f.docId = (SELECT rcmdDocId FROM final_doc)
GROUP BY
    f.docId, y.post_title, y.post_writer;

SELECT YEAR(allrecords.post_date) AS year, COUNT(*) FROM db_proj_datasrc.allrecords WHERE allrecords.post_body LIKE '%North Korea%' AND allrecords.post_body LIKE '%corona%' GROUP BY year ORDER BY year DESC;
select allrecords.top_category, count(*) from db_proj_datasrc.allrecords
GROUP BY allrecords.top_category;


SELECT MONTH(allrecords.post_date) AS month, COUNT(*) AS publication_count
FROM db_proj_datasrc.allrecords
GROUP BY MONTH(allrecords.post_date)
ORDER BY publication_count DESC
LIMIT 1;

SELECT f.docId, f.docTitle, a.post_writer
FROM db_proj_24.year_pub_count_table a
JOIN frequency f ON a.post_title = f.docTitle
JOIN db_proj_24.doc_info_table1  d ON a.post_title = d.doc_title
WHERE d.first_char_title LIKE 'ㅁ'
  AND f.tfidfWord = '고찰'
ORDER BY f.score DESC
LIMIT 1;


WITH tfidf_ranked AS (
    SELECT
        f.docId,
        d.post_body,
        LENGTH(d.post_body) AS post_body_length,
        f.tfidfWord,
        f.score,
        ROW_NUMBER() OVER (PARTITION BY f.docId ORDER BY f.score DESC) AS rn,
        LAG(f.score) OVER (PARTITION BY f.docId ORDER BY f.score DESC) AS prev_tfidfScore
    FROM
        db_proj_datasrc.frequency AS f
        JOIN db_proj_24.year_pub_count_table AS d ON f.docTitle = d.post_title
    WHERE
        LENGTH(d.post_body) > 150
),
predominance_calc AS (
    SELECT
        docId,
        post_body,
        post_body_length,
        tfidfWord,
        score,
        (score - prev_tfidfScore) AS predominance
    FROM
        tfidf_ranked
    WHERE
        prev_tfidfScore IS NOT NULL
),
top_docs AS (
    SELECT
        docId,
        post_body,
        post_body_length,
        tfidfWord,
        score,
        predominance,
        ROW_NUMBER() OVER (ORDER BY predominance DESC) AS overall_rank
    FROM
        predominance_calc
)
SELECT
    docId,
    post_body_length,
    tfidfWord,
    score,
    predominance
FROM
    top_docs
WHERE
    overall_rank <= 10;


SELECT
    f.tfidfWord,
    f.docId,
    f.score / ds.totalTfidf AS representativeness
FROM
    frequency f
JOIN
    (SELECT docId, SUM(score) AS totalTfidf
     FROM frequency
     GROUP BY docId) ds
ON
    f.docId = ds.docId
JOIN
    year_pub_count_table y
ON
    f.docTitle=y.post_title
WHERE
    y.post_writer = '김준형'
ORDER BY
    representativeness DESC;



SELECT table_schema AS 'DatabaseName', ROUND(SUM(data_length+index_length)/1024, 1) AS 'Size(KB)'
FROM information_schema.tables
WHERE table_schema = 'db_proj_24' GROUP BY table_schema;


SELECT TABLE_SCHEMA, TABLE_NAME, ROUND(DATA_LENGTH/(1024), 1) AS 'data(KB)', ROUND(INDEX_LENGTH/(1024), 1) AS 'idx(KB)'
FROM information_schema.tables WHERE TABLE_TYPE = 'BASE TABLE'
AND TABLE_SCHEMA = 'db_proj_24';

SELECT TABLE_SCHEMA, TABLE_NAME,
       ROUND(DATA_LENGTH / 1024, 1) AS 'data(KB)',
       ROUND(INDEX_LENGTH / 1024, 1) AS 'idx(KB)'
FROM information_schema.tables
WHERE TABLE_TYPE = 'BASE TABLE'
AND TABLE_SCHEMA = 'db_proj_24';


select allrecords.post_title, post_date from similarity
JOIN frequency ON frequency.docId=similarity.docId
JOIN db_proj_datasrc.allrecords ON allrecords.doc_title=frequency.docTitle
WHERE (allrecords.email LIKE '****@handong.edu'
           OR allrecords.email LIKE '****@handong.ac.kr')
  AND similarity.score<>1 AND  post_date is not null
ORDER BY post_date desc
LIMIT 1;