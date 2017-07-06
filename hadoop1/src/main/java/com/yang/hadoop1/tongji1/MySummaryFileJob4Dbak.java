package com.yang.hadoop1.tongji1;

import java.io.IOException;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.yang.hadoop1.ByteUtil;
import com.yang.hadoop1.ContentUtil;

public class MySummaryFileJob4Dbak {
	private static	Logger log = LoggerFactory.getLogger(MySummaryFileJob4Dbak.class);
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException, ParseException {
		String confPath = args[0];
		String outputPath = args[1];
		 String jobName = args[2];
		DateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		ContentUtil.init(confPath);
		Configuration config = HBaseConfiguration.create();
		 config.addResource(confPath);
		Job job = new Job(config, jobName);
		job.setJarByClass(MySummaryFileJob4Dbak.class); // class that contains mapper
													// and reducer

		List scans = new ArrayList();
		for (String cloudId : ContentUtil.cloudids) {
			Scan scan = new Scan();
			scan.setCaching(500); // 1 is the default in Scan, which will be bad
			scan.setCacheBlocks(false); // don't set to true for MR jobs
			scan.setAttribute(Scan.SCAN_ATTRIBUTES_TABLE_NAME, "tr_bay".getBytes());
			byte[] startRow = new byte[22];
			ByteUtil.putShort(startRow, Short.valueOf(cloudId), 0);
			ByteUtil.putLong(startRow, df.parse(ContentUtil.getValue("startTime")).getTime(), 2);
			ByteUtil.putString(startRow, "", 10);
			scan.setStartRow(startRow);
			byte[] stopRow = new byte[22];
			ByteUtil.putShort(stopRow,  Short.valueOf(cloudId), 0);
			ByteUtil.putLong(stopRow, df.parse(ContentUtil.getValue("endTime")).getTime(), 2);
			ByteUtil.putString(stopRow, "", 10);
			scan.setStopRow(stopRow);
			scans.add(scan);
			log.info(cloudId+"scan添加成功");
		}
		log.info("scan全部加载完成");
			TableMapReduceUtil.initTableMapperJob(scans, MyMapperPlate1.class, // mapper
					Text.class, // mapper output key
					IntWritable.class, // mapper output value
					job);
			//job.setCombinerClass(MyReducer.class);
			job.setReducerClass(MyReducerPlate1.class); // reducer class
			log.info("启动按天统计完成");
		
		//job.setNumReduceTasks(1); // at least one, adjust as required
		FileOutputFormat.setOutputPath(job, new Path(outputPath)); // adjust

		boolean b = job.waitForCompletion(true);
		if (!b) {
			throw new IOException("error with job!");
		}

	}
}
