#!/bin/bash
rm -r ./result_d
hadoop fs -rmr /tmp/mr/summary_d
hadoop jar ./hadoop1-0.0.1-SNAPSHOT-jar-with-dependencies.jar com.yang.hadoop1.tongji1.MySummaryFileJob4D ./  /tmp/mr/summary_d yyyymmdd4rangdata
hadoop fs -copyToLocal   /tmp/mr/summary_d ./result_d