package com.yang.hadoop1;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.Iterator;
import java.util.List;

import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;

public class CleanData {
	
	 
	 
	 public static void main(String[] args) {
		//ls
		 //ContentUtil.init(args[0]);
		 DateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		   HTable tr_bay =  HTableProxy.getHTable("tr_bay");
		   HTable tr_plat =  HTableProxy.getHTable("tr_plate");
		   List< Delete>  tr_bay_keys = new ArrayList< Delete>();
		   List< Delete>  tr_plate_keys = new ArrayList< Delete>();
		   String[] kks = ContentUtil.getValue("clean_kk").split(",");
		   List<String> kksList = Arrays.asList(kks);
		try {
			System.out.println(ContentUtil.getValue("clean_plate"));
			System.out.println(ContentUtil.getValue("clean_startTime"));
			System.out.println(ContentUtil.getValue("clean_endTime"));
			System.out.println(ContentUtil.getValue("clean_kk"));
			ResultScanner scanner = null;
			Scan scan = new Scan();
			scan.addFamily(Bytes.toBytes("cf"));
			scan.setCaching(100000);	
			byte[] rowkey = new byte[22];
			ByteUtil.putString(rowkey, ContentUtil.getValue("clean_plate"),  0);
			ByteUtil.putLong(rowkey, df.parse(ContentUtil.getValue("clean_startTime")).getTime(), 12);
			ByteUtil.putShort(rowkey, Short.MIN_VALUE, 20);
			scan.setStartRow(rowkey);
			byte[] rowkey2 = new byte[22];
			ByteUtil.putString(rowkey2, ContentUtil.getValue("clean_plate"),  0);
			ByteUtil.putLong(rowkey2, df.parse(ContentUtil.getValue("clean_endTime")).getTime(), 12);
			ByteUtil.putShort(rowkey2, Short.MAX_VALUE, 20);
			scan.setStopRow(rowkey2);
			scanner = tr_plat.getScanner(scan);
			Iterator<Result> it = scanner.iterator();
			int count=0;
			while(it.hasNext()){
				Result r = it.next();
				byte[] key =r.getRow() ;
				if("all".equals(ContentUtil.getValue("clean_kk"))||kksList.contains(""+ByteUtil.getShort(key, 20))){
					tr_plate_keys.add(new Delete(key));
					byte[] bayKey = new byte[22];
					ByteUtil.putShort(bayKey, ByteUtil.getShort(key, 20), 0);
					ByteUtil.putLong(bayKey, ByteUtil.getLong(key, 12), 2);
					ByteUtil.putString(bayKey, ByteUtil.getString(key, 0), 10);
					tr_bay_keys.add(new Delete(bayKey));
					System.out.println(ByteUtil.getString(key, 0) +"_过车时间"+ df.format(new Date(ByteUtil.getLong(key, 12))) +"_卡点号"+ByteUtil.getShort(key, 20));
					count++;
				}
			}
			System.out.println("数据展示完毕总共"+count+"条");
			if(scanner!=null){
				scanner.close();
			}
			
			BufferedReader strin=new BufferedReader(new InputStreamReader(System.in)); 
			System.out.println("是否删除查到的数据,删除数据输入y,不删除输入其他:");
			String  is = strin.readLine();
			if("y".equals(is)){
				tr_bay.delete(tr_bay_keys);
				tr_plat.delete(tr_plate_keys);
				System.out.println("删除完成");
			}else{
				System.out.println("没有删除数据程序退出");
				}
		} catch (ParseException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}	finally{
			if(tr_plat!=null){
				try {
					tr_plat.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
			
			if(tr_bay!=null){
				try {
					tr_plat.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
		}
	}
}
