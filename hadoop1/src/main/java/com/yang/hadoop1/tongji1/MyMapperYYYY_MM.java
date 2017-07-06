package com.yang.hadoop1.tongji1;

import java.io.IOException;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;

import com.yang.hadoop1.ByteUtil;

public  class MyMapperYYYY_MM extends TableMapper<Text, IntWritable>  {

	private final IntWritable ONE = new IntWritable(1);
   	private Text text = new Text();
   	private DateFormat df = new SimpleDateFormat("yyyy-MM");
   	private DateFormat dfs = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
   	public void map(ImmutableBytesWritable row, Result value, Context context) throws IOException, InterruptedException {
   		try {
			Long startTime = dfs.parse(context.getConfiguration().get("startTime")).getTime();
			Long stopTime = dfs.parse(context.getConfiguration().get("stopTime")).getTime();
			byte[] bs =value.getRow();
			Long dataTime =ByteUtil.getLong(bs, 2);
			if(startTime<=dataTime && stopTime>=dataTime){
				String val = df.format(new Date(dataTime));
	          	text.set(val);
	        	context.write(text, ONE);
			}
		} catch (ParseException e) {
			e.printStackTrace();
		}
   	}
}