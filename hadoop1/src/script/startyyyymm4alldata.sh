#!/bin/bash
rm -r ./result_m
hadoop fs -rmr /tmp/mr/summary_m
hadoop jar ./hadoop1-0.0.1-SNAPSHOT-jar-with-dependencies.jar com.yang.hadoop1.tongji1.MySummaryFileJob4M ./  /tmp/mr/summary_m yyyymm4alldata
hadoop fs -copyToLocal   /tmp/mr/summary_m ./result_m