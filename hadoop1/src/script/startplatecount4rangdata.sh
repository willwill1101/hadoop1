#!/bin/bash
rm -r ./result_p
hadoop fs -rmr /tmp/mr/summary_p
hadoop fs -rmr /tmp/mr/job1/path
hadoop jar ./hadoop1-0.0.1-SNAPSHOT-jar-with-dependencies.jar com.yang.hadoop1.tongji1.MySummary4Plate ./  /tmp/mr/summary_p plate4rangdata
hadoop fs -copyToLocal   /tmp/mr/summary_p ./result_p