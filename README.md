# HBase.GetTopNRecords
## Overview
Say you have 100 items each with a amount and you want to get the top 5 items in order of amount with a single HBase get.  That is what this code does.

This code comes with two modes: A put mode and get mode.  The idea is to run them in different consoles so one is updating the items and amounts and the other is getting the latest results.  

##How to Build
Simple mvn package will work

##How to execute

Hadoop jar HBaseTop5Getting put <tableName> <columnFamily> <numberOfMSecondsIntervals> <repeatNTimes> <numberOfItems> <TopNToRetain>
or
Hadoop jar HBaseTop5Getting put topN cf 1000 100 100 5


Hadoop jar HBaseTop5Getting get <tableName> <columnFamily> <numberOfMSecondsIntervals> <repeatNTimes> <TopNToRetain>
or
Hadoop jar HBaseTop5Getting get topN cf 1000 100 5
      
