-- VGI-Java quickstart — run in a Haybarn shell.
--
-- Prereq: build the worker first (`./gradlew installDist` in ../), then replace
-- the LOCATION path below with the absolute path printed by `../run.sh`.
--
-- The vgi extension must be available:
INSTALL vgi FROM community;
LOAD vgi;

-- 'launch:' starts the JVM worker once and pools it across queries.
ATTACH 'demo' AS demo (TYPE vgi,
    LOCATION 'launch:/ABSOLUTE/PATH/TO/build/install/vgi-java-examples/bin/vgi-java-examples');

-- scalar — one row in, one row out
SELECT demo.upper_case('hello');                              -- HELLO

-- table — a set-returning generator, streamed in batches
SELECT * FROM demo.numbers(5) ORDER BY n;                     -- 0,1,2,3,4
SELECT count(*) FROM (SELECT * FROM demo.numbers(1000000) LIMIT 7);  -- 7 (LIMIT pushdown)

-- table-in-out — a streaming relation transform
SELECT n FROM demo.echo((SELECT * FROM demo.numbers(3))) ORDER BY n;  -- 0,1,2

-- aggregate — parallel partial aggregation
SELECT g, demo.vgi_sum(v)
  FROM (VALUES (1,10),(1,20),(2,5)) t(g,v) GROUP BY g ORDER BY g;     -- 1->30, 2->5

-- buffering — must see all input before producing output
SELECT n FROM demo.collect((SELECT * FROM demo.numbers(4))) ORDER BY n;  -- 0,1,2,3

DETACH demo;
