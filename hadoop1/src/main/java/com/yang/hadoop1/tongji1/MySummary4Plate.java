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
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.yang.hadoop1.ByteUtil;
import com.yang.hadoop1.ContentUtil;

public class MySummary4Plate {
	private static Logger log = LoggerFactory.getLogger(MySummary4Plate.class);

	public static void main(String[] args)
			throws IOException, ClassNotFoundException, InterruptedException, ParseException {
		String confPath = args[0];
		String outputPath = args[1];
		String jobName = args[2];
		String step = args[3];
		String outputPath1 = "/tmp/mr/job1/path";
		DateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		ContentUtil.init(confPath);
		Configuration config = HBaseConfiguration.create();
		 config.set("startTime", ContentUtil.getValue("startTime"));
		 config.set("stopTime", ContentUtil.getValue("stopTime"));
		if("1".equals(step)){
		 Job job1 = new Job(config, jobName+1);
		job1.setJarByClass(MySummary4Plate.class); // class that contains mapper
													// and reducer


		 Scan scan = new Scan();
		 scan.setCaching(500);        // 1 is the default in Scan, which will be bad for MapReduce jobs
		 scan.setCacheBlocks(false);  // don't set to true for MR jobs

		 TableMapReduceUtil.initTableMapperJob(
		 	"tr_bay",        // input table
		 	scan,               // Scan instance to control CF and attribute selection
		 	MyMapperPlate1.class,     // mapper class
		 	Text.class,         // mapper output key
		 	IntWritable.class,  // mapper output value
		 	job1);
		job1.setReducerClass(MyReducerPlate1.class); // reducer class
		log.info("启动统计车牌数统计完成");

		FileOutputFormat.setOutputPath(job1, new Path(outputPath1)); // adjust
		boolean b = job1.waitForCompletion(true);
		if (!b) {
			throw new IOException("error with job1!");
		}
		 }
			System.out.println("job第一阶段完成");
			//----------------------------------------------job2
			Configuration conf = new Configuration();
			Job job2 = new Job(config,jobName+2);
			job2.setJarByClass(MySummary4Plate.class); // class that contains mapper
			job2.setMapperClass(MyMapperPlate2.class);
			job2.setMapOutputKeyClass(Text.class);  
		    job2.setMapOutputValueClass(IntWritable.class);  
			job2.setCombinerClass(MyReducer.class);
			job2.setReducerClass(MyReducer.class);
			job2.setOutputKeyClass(Text.class);
			job2.setOutputValueClass(IntWritable.class);
			job2.setNumReduceTasks(1);
			FileInputFormat.addInputPath(job2, new Path(outputPath1));
			FileOutputFormat.setOutputPath(job2, new Path(outputPath));
			boolean b2 = job2.waitForCompletion(true);
			if (!b2) {
				throw new IOException("error with job2!");
			}

	}
}
