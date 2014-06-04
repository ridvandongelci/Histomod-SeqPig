#!/usr/bin/python
from __main__ import *
from org.apache.pig.scripting import *
from datetime import datetime
import shutil
from subprocess import call
import sys
import os

home = "/homeappl/home/dongelr1/"

pig_home= home+"pig-0.12.1/"
hadoo_home = home+"hadoop-2.2.0/"
seqpig_home = home+"SeqPig/"

data_home_local = home+"data/share.ics.aalto.fi/project/csb/mpirttin/SeqPig/"
data_home = ""
bed_local = home+"data/share.ics.aalto.fi/project/csb/mpirttin/SeqPig/GR_Enhancers.bed"
bed = "GR_Enhancers.bed"

local = False

jars = [seqpig_home+"lib/gephi-toolkit.jar",
        seqpig_home+"lib/sam-1.107.jar",
        seqpig_home+"lib/tribble-1.107.jar",
        seqpig_home+"lib/hadoop-bam-6.2.0-SNAPSHOT.jar",
        seqpig_home+"lib/seal.jar",
        seqpig_home+"lib/picard-1.107.jar",
        seqpig_home+"lib/variant-1.107.jar",
        pig_home+"contrib/piggybank.jar",
        seqpig_home+"build/jar/SeqPig.jar",
        seqpig_home+"lib/datafu-1.2.0.jar",
        seqpig_home+"lib/commons-logging-1.1.1.jar",
        seqpig_home+"lib/commons-jexl-2.1.1.jar"]
        		
udfs = ["fi.aalto.seqpig","fi.aalto.seqpig.io","fi.aalto.seqpig.filter","fi.aalto.seqpig.pileup","fi.aalto.seqpig.stats","myudf"]


window = 5000
bedtemp = "bedtemp"

#os.system(["hadoop fs -rmr "+bedtemp])
try:
	if not local:
		os.system([hadoo_home+"/bin/hadoop fs -copyFromLocal "+bed_local+" "+bed])
except:
	e = sys.exc_info()[0]
        print( "<p>Error: %s</p>" % e )

try:
	if(local):
		shutil.rmtree(bedtemp)
	else:
		os.system([hadoo_home+"/bin/hadoop fs -rmr"+" "+ bedtemp])
except:
	e = sys.exc_info()[0]
	print( "<p>Error: %s</p>" % e )
 
 
 
start = datetime.now()
print(start)
P = Pig.compileFromFile("bedtoregionlist3.pig")
Q = P.bind({'bed':bed, 'output':bedtemp})
 
bedresult = Q.runSingle(); 
 
chrms = set()
rlist = "";
 
iterator = bedresult.result("BED").iterator()
while iterator.hasNext():
    line = iterator.next();
    value = str(line.get(0))
    endindex = int(str(line.get(2)))
    rlist += value+":"+str(endindex-window)+"-"+str(endindex+window-1)+","
    if value not in chrms:
        chrms.add(value)
 
rlist = rlist[:-1]

	

data = ['wgEncodeBroadHistoneK562ControlStdRawData',
        'wgEncodeSydhTfbsK562InputV2RawData',
        'wgEncodeBroadHistoneK562CtcfStdRawData',
        'wgEncodeBroadHistoneK562H2azStdRawData',
        'wgEncodeBroadHistoneK562H3k27acStdRawData',
        'wgEncodeBroadHistoneK562H3k27me3StdRawData',
        'wgEncodeBroadHistoneK562H3k36me3StdRawData',
        'wgEncodeBroadHistoneK562H3k4me1StdRawData',
        'wgEncodeBroadHistoneK562H3k4me2StdRawData',
        'wgEncodeBroadHistoneK562H3k4me3StdRawData',
        'wgEncodeBroadHistoneK562H3k79me2StdRawData',
        'wgEncodeBroadHistoneK562H3k9acStdRawData',
        'wgEncodeBroadHistoneK562H3k9me3StdRawData',
        'wgEncodeBroadHistoneK562H4k20me1StdRawData',
        'wgEncodeSydhTfbsK562Pol2RawData',
        'DNase-seq/bam_format',
        'NucleosomeSYDH/bam_format']

if not local:
	for f in data:
		os.system([hadoo_home+"/bin/hadoop fs -mkdir -p"+" "+f])
		os.system([hadoo_home+"/bin/hadoop fs -copyFromLocal "+data_home_local+"/"+f+" "+f])
	
chromfiles = "{"+(",".join(chrms))+"}.sorted.bam"
 
bindparam = []
for dataset in data:
    try:
    	if(local):
    		shutil.rmtree(data_home+dataset+'.matrix')
    	else:
    		os.system([hadoo_home+"/bin/hadoop fs -rmr"+" "+ data_home+dataset+'.matrix'])
    except:
    	e = sys.exc_info()[0]
    	print( "<p>Error: %s</p>" % e )
    try:
    	if(local):
    		shutil.rmtree(data_home+dataset+'.matrix.count')
    	else:
    		os.system([hadoo_home+"/bin/hadoop fs -rmr"+" "+ data_home+dataset+'.matrix.count'])
    except:
    	e = sys.exc_info()[0]
    	print( "<p>Error: %s</p>" % e )
    bindparam.append({'bam':(data_home+dataset+'/'+chromfiles),
                   'rlist':rlist, 'bed':bed, 'window':window,
                   'output':(data_home+dataset+'.matrix')})
#print(bindparam)
for jar in jars:
    Pig.registerJar(jar)
# for udf in udfs:
#     Pig.registerUDF("",udf)
P2 = Pig.compileFromFile("bamprocessing3.pig")
Q2 = P2.bind(bindparam[0:3])
bamresult =Q2.run()
end = datetime.now()
# 
print(end-start)
