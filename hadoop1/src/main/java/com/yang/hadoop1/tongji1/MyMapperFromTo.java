package com.yang.hadoop1.tongji1;

import java.io.IOException;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Date;

import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;

import com.yang.hadoop1.ByteUtil;

public  class MyMapperFromTo extends TableMapper<Text, IntWritable>  {

	private final IntWritable ONE = new IntWritable();
   	private Text text = new Text();
   	private DateFormat df = new SimpleDateFormat("yyyy-MM-dd");
   	private DateFormat dfs = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
   	public static final Integer[] CAR_PLATE_NUMBER = new Integer[]{11,21};
	public static final Integer[] CAR_PLATE_TYPE = new Integer[]{35,36};
   	public void map(ImmutableBytesWritable row, Result value, Context context) throws IOException, InterruptedException {
        	
   		
   		try {
			Long startTime = dfs.parse(context.getConfiguration().get("startTime")).getTime();
			Long stopTime = dfs.parse(context.getConfiguration().get("stopTime")).getTime();
			byte[] bs =value.getRow();
			Long dataTime =ByteUtil.getLong(bs, 2);
			if(startTime<=dataTime && stopTime>=dataTime){
			
				byte[] column =value.getColumn("cf".getBytes(), null).get(0).getValue();
				
				String plate_num = new String(Arrays.copyOfRange(column, CAR_PLATE_NUMBER[0], CAR_PLATE_NUMBER[1]),"GBK");
				
				byte plate_type_byte = Arrays.copyOfRange(column, CAR_PLATE_TYPE[0], CAR_PLATE_TYPE[1])[0];
				int plate_type = (plate_type_byte < 0 ? 127 - plate_type_byte : plate_type_byte);
				
				
				String time = df.format(new Date(dataTime));
			  	
				text.set(time+"@"+plate_num.trim()+"_"+plate_type);
				context.write(text, ONE);
			}
		} catch (ParseException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
   	}
}