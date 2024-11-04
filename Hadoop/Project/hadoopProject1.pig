-- Load data from HDFS
input4 = LOAD 'hdfs:///user/sruthi/inputs/episodeIV_dialogues.txt' USING PigStorage('\t') AS (name:chararray, line:chararray);
input5 = LOAD 'hdfs:///user/sruthi/inputs/episodeV_dialogues.txt' USING PigStorage('\t') AS (name:chararray, line:chararray);
input6 = LOAD 'hdfs:///user/sruthi/inputs/episodeVI_dialogues.txt' USING PigStorage('\t') AS (name:chararray, line:chararray);

-- Filter out the first 2 lines from each file
ranked4 = RANK input4;
OnlyDialogues4 = FILTER ranked4 BY (rank_input4 > 2);
ranked5 = RANK input5;
OnlyDialogues5 = FILTER ranked5 BY (rank_input5 > 2);
ranked6 = RANK input6;
OnlyDialogues6 = FILTER ranked6 BY (rank_input6 > 2);

-- Merge the three inputs
inputData = UNION OnlyDialogues4, OnlyDialogues5, OnlyDialogues6;

-- Group by name
groupByName = GROUP inputData BY name;

-- Count the number of lines by each character
names = FOREACH groupByName GENERATE $0 as name, COUNT($1) as no_of_lines;
namesOrdered = ORDER names BY no_of_lines DESC;

-- Remove the outputs folder
rmf hdfs:///user/sruthi/outputs;

-- Store result in HDFS
STORE namesOrdered INTO 'hdfs:///user/sruthi/outputs' USING PigStorage('\t');