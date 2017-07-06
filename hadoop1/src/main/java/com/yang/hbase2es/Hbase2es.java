package com.yang.hbase2es;

import java.io.IOException;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.PageFilter;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.yang.hadoop1.ContentUtil;
import com.yang.hadoop1.tongji1.MyReducerPlate1;

public class Hbase2es {
	private static Logger log = LoggerFactory.getLogger(Hbase2es.class);

	public static void main(String[] args)
			throws IOException, ClassNotFoundException, InterruptedException, ParseException {
		String jobName = args[0];
		String outputPath1 = "/tmp/mr/hbase2es/path/"+args[1];
		DateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		ContentUtil.init(Hbase2es.class.getClassLoader().getResourceAsStream("conf.properties"));
		Configuration config = HBaseConfiguration.create();
		Job job1 = new Job(config, jobName);
		job1.setJarByClass(Hbase2es.class); // class that contains mapper
											// and reducer

		Scan scan = new Scan();
		//Filter filter = new PageFilter(500);
		//can.setFilter(filter);
		scan.setCaching(500); // 1 is the default in Scan, which will be bad for
								// MapReduce jobs
		scan.setCacheBlocks(false); // don't set to true for MR jobs

		TableMapReduceUtil.initTableMapperJob("tr_bay", // input table
				scan, // Scan instance to control CF and attribute selection
				MyMapper.class, // mapper class
				Text.class, // mapper output key
				IntWritable.class, // mapper output value
				job1);
		job1.setReducerClass(MyReducerPlate1.class); // reducer class
		log.info("启动完成");

		FileOutputFormat.setOutputPath(job1, new Path(outputPath1)); // adjust
		boolean b = job1.waitForCompletion(true);
		if (!b) {
			throw new IOException("error with job1!");
		}

	}
}
