Project = LOAD '/home/hduser/PigTask/part-r-00000.txt' using PigStorage(',') as (fld1:chararray, fld2:chararray, fld3:chararray, fld4:int);

fld1_fld4_values = FOREACH Project GENERATE fld1, fld4;

fld4_fld1_groupby2 = GROUP fld1_fld4_values BY (fld1);

fld4_avg = FOREACH fld4_fld1_groupby2 GENERATE group, AVG(fld1_fld4_values.fld4);

DUMP fld4_avg;

STORE fld4_avg INTO 'PRJ' USING PigStorage (',');

