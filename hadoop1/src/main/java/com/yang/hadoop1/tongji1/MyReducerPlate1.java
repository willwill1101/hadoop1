package com.yang.hadoop1.tongji1;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.Reducer;

public  class MyReducerPlate1 extends Reducer<Text, IntWritable, Text, IntWritable>  {
	public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
		context.write(key, null);
	}
}
