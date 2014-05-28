BED = load '$bed' using PigStorage('\t') as (chrname:chararray,startindex:int,endindex:int);
store BED into '$output';