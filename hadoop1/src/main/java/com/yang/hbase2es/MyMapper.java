package com.yang.hbase2es;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;

import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counter;

import com.ehl.dataselect.business.server.TR2Result;
import com.ehl.dataselect.hbase.persistent.TravelRecord;
import com.ehl.tvc.realtime.esbolt.bean.TrPlateEsBean;
import com.ehl.tvc.realtime.esbolt.dao.TravelRecordESDao;

public  class MyMapper extends TableMapper<Text, IntWritable>  {

   	private final static	byte[] cf = "cf".getBytes();
   	private static SimpleDateFormat format = new SimpleDateFormat("yyyy-MM");
   	private static TravelRecordESDao dao = new TravelRecordESDao();
   	public void map(ImmutableBytesWritable row, Result value, Context context) throws IOException, InterruptedException {
   		try {
   			
			byte[] column =value.getColumn(cf, null).get(0).getValue();
			TravelRecord tr = TravelRecord.fromBytes(column);
			TrPlateEsBean resultCar =TR2Result.getResultCar(tr, false,false);
			dao.saveTravelRecord(resultCar);
			final Counter counter = context.getCounter("hbase2es", format.format(new Date(resultCar.getTimestamp())));  
			counter.increment(1l);
		} catch (Exception e) {
			e.printStackTrace();
		}
   	}
}