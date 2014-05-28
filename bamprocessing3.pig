BAM = load '$bam' using fi.aalto.seqpig.io.BamLoader('yes');
define regionHitIns myudf.RegionHit('$rlist');

BAM_IN = foreach BAM generate FLATTEN(regionHitIns(refname,start,end)),read, flags, refname,start, cigar, basequal;
BAMG = group BAM_IN by ($0,$1,$2);

BED = load '$bed' using PigStorage('\t') as (chrname:chararray,startindex:int,endindex:int);
BEDG = foreach BED generate chrname,endindex-$window,endindex+$window-1;

ALLG = JOIN BEDG by ($0,$1,$2) LEFT, BAMG by ($0.$0,$0.$1,$0.$2);

A = foreach ALLG generate FLATTEN(myudf.MatrixGenerator2($1,$2,$4));


store A  into '$output' using PigStorage(',');

CHRGRP = group BAM by refname;
COUNT = foreach CHRGRP generate $0, COUNT(BAM);

store COUNT into '$output.count';