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

public class MySummaryFileJob4D {
	private static	Logger log = LoggerFactory.getLogger(MySummaryFileJob4D.class);
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException, ParseException {
		String confPath = args[0];
		String outputPath = args[1];
		 String jobName = args[2];
		DateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		ContentUtil.init(confPath);
		Configuration config = HBaseConfiguration.create();
		 config.set("startTime", ContentUtil.getValue("startTime"));
		 config.set("stopTime", ContentUtil.getValue("stopTime"));
		Job job = new Job(config, jobName);
		job.setJarByClass(MySummaryFileJob4D.class); // class that contains mapper
													// and reducer

		 Scan scan = new Scan();
		 scan.setCaching(500);        // 1 is the default in Scan, which will be bad for MapReduce jobs
		 scan.setCacheBlocks(false);  // don't set to true for MR jobs

		 TableMapReduceUtil.initTableMapperJob(
		 	"tr_bay",        // input table
		 	scan,               // Scan instance to control CF and attribute selection
		 	MyMapperYYYY_MM_DD.class,     // mapper class
		 	Text.class,         // mapper output key
		 	IntWritable.class,  // mapper output value
		 	job);
		 job.setCombinerClass(MyReducer.class);
		 job.setReducerClass(MyReducer.class);    // reducer class
		 job.setNumReduceTasks(1);    // at least one, adjust as required
		 FileOutputFormat.setOutputPath(job, new Path(outputPath));  // adjust directories as required

		boolean b = job.waitForCompletion(true);
		if (!b) {
			throw new IOException("error with job!");
		}

	}
}
