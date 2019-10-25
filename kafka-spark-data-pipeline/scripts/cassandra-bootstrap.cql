CREATE KEYSPACE vocabulary
    WITH REPLICATION = {
        'class' : 'SimpleStrategy',
        'replication_factor' : 1
    };
USE vocabulary;
CREATE TABLE words (word text PRIMARY KEY, count int);
