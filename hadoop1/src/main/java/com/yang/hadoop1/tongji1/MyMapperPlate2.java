package com.yang.hadoop1.tongji1;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public  class MyMapperPlate2 extends Mapper<LongWritable,Text,Text,IntWritable>  {
	IntWritable ONE = new IntWritable(1);

	@Override
	protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, IntWritable>.Context context)
			throws IOException, InterruptedException {
		String keyStr = value.toString();
		try {
			String[] keys =  keyStr.split("@");
			context.write(new Text(keys[0]), ONE);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}


	
}