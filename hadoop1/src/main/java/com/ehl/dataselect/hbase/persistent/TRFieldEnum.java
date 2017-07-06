package com.ehl.dataselect.hbase.persistent;

import java.util.HashMap;


/**
 * 
 * 涵盖一条过车记录的各属性信息，每个属性包含5个子属性和6个方法，描述见底部枚举本身的方法
 * 
 * 
 */
public class TRFieldEnum {
	
	public static final Version VERSION = new Version("VERSION",1,0);
	public static final Timestamp TIMESTAMP = new Timestamp("TIMESTAMP", 8,1);
	public static final Order_number ORDER_NUMBER = new Order_number("ORDER_NUMBER", 2,9);
	public static final Car_plate_number CAR_PLATE_NUMBER = new Car_plate_number("CAR_PLATE_NUMBER", 10,11);
	public static final Speed SPEED = new Speed("SPEED", 2,21);
	public static final Lane_id LANE_ID = new Lane_id("LANE_ID", 4,23);
	public static final Camera_location CAMERA_LOCATION = new Camera_location("CAMERA_LOCATION", 2, true,27);
	public static final Bay_id BAY_ID = new Bay_id("BAY_ID", 2, true,29);
	public static final Camera_orientation CAMERA_ORIENTATION = new Camera_orientation("CAMERA_ORIENTATION", 1, true,31);
	public static final Car_brand CAR_BRAND = new Car_brand("CAR_BRAND", 1, true,32);
	public static final Car_color CAR_COLOR = new Car_color("CAR_COLOR", 1, true,33);
	public static final Car_plate_color CAR_PLATE_COLOR = new  Car_plate_color("CAR_PLATE_COLOR", 1, true,34);
	public static final Car_plate_type CAR_PLATE_TYPE = new Car_plate_type("CAR_PLATE_TYPE", 1, true,35);
	public static final Car_status CAR_STATUS = new Car_status("CAR_STATUS", 1, true,36);
	public static final Travel_orientation TRAVEL_ORIENTATION = new Travel_orientation("TRAVEL_ORIENTATION", 1, true,37);
	public static final Plate_coordinates PLATE_COORDINATES = new Plate_coordinates("PLATE_COORDINATES", 8,38);
	public static final Driver_coordinates DRIVER_COORDINATES = new Driver_coordinates("DRIVER_COORDINATES", 8,46);
	public static final Image_urls IMAGE_URLS = new  Image_urls("IMAGE_URLS", -1,54);
	
	private static  HashMap<String,TRFieldBase> map = new HashMap<String,TRFieldBase>();
//	private static  HashMap<String,TRFieldBase> map = new HashMap<String,TRFieldBase>();
	
	static{
		map.put(VERSION.NAME,VERSION);
		map.put(TIMESTAMP.NAME,TIMESTAMP);
		map.put(ORDER_NUMBER.NAME,ORDER_NUMBER);
		map.put(CAR_PLATE_NUMBER.NAME,CAR_PLATE_NUMBER);
		map.put(SPEED.NAME,SPEED);
		map.put(LANE_ID.NAME,LANE_ID);
		map.put(CAMERA_LOCATION.NAME,CAMERA_LOCATION);
		map.put(BAY_ID.NAME,BAY_ID);
		map.put(CAMERA_ORIENTATION.NAME,CAMERA_ORIENTATION);
		map.put(CAR_BRAND.NAME,CAR_BRAND);
		map.put(CAR_COLOR.NAME,CAR_COLOR);
		map.put(CAR_PLATE_COLOR.NAME,CAR_PLATE_COLOR);
		map.put(CAR_PLATE_TYPE.NAME,CAR_PLATE_TYPE);
		map.put(CAR_STATUS.NAME,CAR_STATUS);
		map.put(TRAVEL_ORIENTATION.NAME,TRAVEL_ORIENTATION);
		map.put(PLATE_COORDINATES.NAME,PLATE_COORDINATES);
		map.put(DRIVER_COORDINATES.NAME,DRIVER_COORDINATES);
		map.put(IMAGE_URLS.NAME,IMAGE_URLS);
	}
	
	private static TRFieldBase[] tr = new TRFieldBase[18];
	static{
		tr[0] = VERSION;
		tr[1] = TIMESTAMP;
		tr[2] = ORDER_NUMBER;
		tr[3] = CAR_PLATE_NUMBER;
		tr[4] = SPEED;
		tr[5] = LANE_ID;
		tr[6] = CAMERA_LOCATION;
		tr[7] = BAY_ID;
		tr[8] = CAMERA_ORIENTATION;
		tr[9] = CAR_BRAND;
		tr[10] = CAR_COLOR;
		tr[11] = CAR_PLATE_COLOR;
		tr[12] = CAR_PLATE_TYPE;
		tr[13] = CAR_STATUS;
		tr[14] = TRAVEL_ORIENTATION;
		tr[15] = PLATE_COORDINATES;
		tr[16] = DRIVER_COORDINATES;
		tr[17] = IMAGE_URLS;
	}
	public static TRFieldBase[] values(){
		return tr;
	}
	
	public static TRFieldBase valueOf(String name) {
		return map.get(name);
	}
	
	
	
	
	
	/**
	 * Arrange the position of each field in the byte array, and at the same time calculate<br>
	 * the total size of all fixed fields.
	 * 获取各属性长度和，并计算各属性偏移量
	 * @return the total fixed size
	 */
	public static int getFixedSize() {
		return 54;
	}
	
}