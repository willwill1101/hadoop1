package com.ehl.dataselect.hbase.persistent;

import java.text.SimpleDateFormat;
import org.apache.hadoop.hbase.util.Bytes;

import com.ehl.dataselect.hbase.util.TableTip;

/**
 * This class plays an important role, which not only serves as the bean of
 * travel record but<br>
 * also provides methods transforming from real value to byte array and
 * opposite.
 * 过车记录信息
 * @author SDU
 * 
 */
public class TravelRecord {
	private byte[] value = null; //过车记录的byte数组，一条数据
	private byte[] rowkey = null;//过车记录的rowkey，查询图片用

	public byte[] getRowkey() {
		return rowkey;
	}

	public void setRowkey(byte[] rowkey) {
		this.rowkey = rowkey;
	}

	/**
	 * From HBase.
	 * 
	 * @param value
	 */
	public TravelRecord(final byte[] value) {
		this.value = value;
	}

	public TravelRecord() {
		final int extraRoom = 3;
		final byte[] bs = new byte[TRFieldEnum.getFixedSize() + extraRoom];
		for (final TRFieldBase field : TRFieldEnum.values()) {
			field.fillBytes(bs, null);
		}
		this.value = bs;
	}
	

	/**
	 * Get a long value of a field.
	 * 读取对应属性的Long值
	 * @param field
	 *            the field
	 * @return the long value
	 */
	public long getLongValue(final TRFieldBase field) {
		return (Long) field.readBytes(value);
	}

	/**
	 * Get a string value of a field.
	 * 读取对应属性的字符值
	 * @param field
	 *            the field
	 * @return the string value
	 */
	public String getStringValue(final TRFieldBase field) {
		return String.valueOf(field.readBytes(value));
	}

	/**
	 * Get an element of a string array.
	 * 获取属性的字符数组数据中的指定位置数据
	 * @param field
	 *            the field
	 * @param index
	 *            the offset of element
	 * @return the value of element
	 */
	public String getStringValueOfStrings(final TRFieldBase field,
			final int index) {
		if (index < 0)
			throw new IllegalArgumentException("Index can not be negative: "
					+ index);
		final Object obj = field.readBytes(value);
		if (obj == null)
			return null;
		else {
			final String[] urls = (String[]) obj;
			if (urls.length <= index)
				throw new IllegalArgumentException("Index " + index
						+ " must be less than length " + urls.length);
			else
				return urls[index];
		}
	}

	/**
	 * Get a string array value of a field.
	 * 获取属性的字符数组数据
	 * @param field the field
	 * @return the string array
	 */
	public String[] getStringArrayValue(final TRFieldBase field) {
		return (String[]) field.readBytes(value);
	}
	
	
	public int[] getIntArrayValue(final TRFieldBase field) {
		return (int[]) field.readBytes(value);
	}

	/**
	 * 
	 * 获取属性的int型数据
	 * @param field   the field
	 * @return the int value
	 */
	public int getIntValue(final TRFieldBase field) {
		final Object obj = field.readBytes(value);
		if (obj instanceof Short)
			return ((Short) obj).shortValue();
		else
			return (Integer) field.readBytes(value);
	}

	/**
	 * Set the value of a field.
	 * 
	 * @param field
	 *            the field
	 * @param value
	 *            the value to be set
	 */
	public void setValue(final TRFieldBase field, final Object value) {
		if (field != TRFieldEnum.IMAGE_URLS) {
			field.fillBytes(this.value, value);
		} else {
			final String[] imageUrls = (String[]) value;
			int extraRoom = 0;
			if (imageUrls != null) {
				for (final String imageUrl : imageUrls) {
					extraRoom += ByteUtil.calBytesLengthForString(imageUrl);
				}
			}
			if (extraRoom == 0) {
				extraRoom = 3;
			}
			final byte[] bs = new byte[TRFieldEnum.getFixedSize() + extraRoom];
			System.arraycopy(this.value, 0, bs, 0, TRFieldEnum.getFixedSize());
			TRFieldEnum.IMAGE_URLS.fillBytes(bs, imageUrls);
			this.value = bs;
		}
	}
	/**
	 * 把图片数据追加到value中，保持访问方式不变
	 */
	public void setURLValue(byte[] imgvalue){
		int textlength = this.value.length;
		int imglength = imgvalue.length;
		byte[] bs = new byte[textlength+imglength];
		System.arraycopy(this.value, 0, bs, 0, textlength);
		System.arraycopy(imgvalue, 0, bs, textlength,imglength );
		this.value = bs;
	}

	/**
	 * Construct an instance of this class according to a byte array.
	 * 
	 * @param value
	 *            the byte array
	 * @return a new instance
	 */
	public static TravelRecord fromBytes(final byte[] value) {
		return new TravelRecord(value);
	}

	/**
	 * Get the byte array value of an instance of this class.
	 * 
	 * @return the byte array value
	 */
	public byte[] toBytes() {
		return value;
	}

	/**
	 * Calculate the rowkey of this instance according to some tips.
	 * 这是使用卡口/车牌 +时间 合成rowKey的方法
	 * @param tip
	 *            the HBase table
	 * @return the corresponding rowkey
	 */
	public byte[] calRowkey(final TableTip tip) {
		byte[] rowkey = new byte[22];
		String number = getStringValue(TRFieldEnum.CAR_PLATE_NUMBER);
		short bid = Short.parseShort(getStringValue(TRFieldEnum.BAY_ID));
		long ts = getLongValue(TRFieldEnum.TIMESTAMP);
		
		if (tip == null || tip == TableTip.BAY_T){
			ByteUtil.putShort(rowkey, bid, 0);
			ByteUtil.putLong(rowkey, ts, 2);
			if(number != null && !number.equals("null")){
				ByteUtil.putString(rowkey, number, 10);
			}
		}else{
			ByteUtil.putString(rowkey, number, 0);
			ByteUtil.putLong(rowkey, ts, 12);
			ByteUtil.putShort(rowkey, bid, 20);
		}
		
		return rowkey;
	}


	/**
	 * Get the primary key of this instance which is unique.
	 * 
	 * @return the primary key
	 */
	public String calCarPK() {
		return getStringValue(TRFieldEnum.CAR_PLATE_NUMBER) + "_"
				+ getIntValue(TRFieldEnum.CAR_PLATE_COLOR);
	}



	/**
	 * Telling whether the value of a field matches the values given by users.
	 * 
	 * @param bs
	 *            the byte array
	 * @param field
	 *            the field
	 * @param value_1
	 *            the first value
	 * @param value_2
	 *            the second value
	 * @return {@code true} if matches, {@code false} otherwise.
	 */
	public static boolean match(final byte[] bs, final TRFieldBase field,
			final Object value_1, final Object value_2) {
		return field.match(bs, value_1, value_2);
	}

	@Override
	public String toString() {
		final StringBuffer sb = new StringBuffer("TravelRecord: [");
		final TRFieldBase[] trfs = TRFieldEnum.values();
		for (int i = 0; i < trfs.length; i++) {
			sb.append(trfs[i].toTRtoString(trfs[i].readBytes(value)));
			if (i < trfs.length - 1)
				sb.append(" * ");
		}
		sb.append("]");
		return sb.toString();
	}

	@Override
	public boolean equals(final Object obj) {
		if (obj == null || obj.getClass() != TravelRecord.class) {
			return false;
		} else {
			final TravelRecord tr2 = (TravelRecord) obj;
			if (this.getLongValue(TRFieldEnum.TIMESTAMP) != tr2
					.getLongValue(TRFieldEnum.TIMESTAMP)) {
				return false;
			} else if (this.getIntValue(TRFieldEnum.ORDER_NUMBER) != this
					.getIntValue(TRFieldEnum.ORDER_NUMBER)) {
				return false;
			} else if (!this.getStringValue(TRFieldEnum.CAR_PLATE_NUMBER)
					.equals(tr2.getStringValue(TRFieldEnum.CAR_PLATE_NUMBER))) {
				return false;
			} else {
				return true;
			}
		}
	}




	/**
	 * According to the raw data coming from kafka, construct a new instance of
	 * this class.
	 * 
	 * @param text
	 *            the raw data
	 * @param filepath1
	 *            the first image url
	 * @param filepath2
	 *            the second image url
	 * @param divide
	 *            whether the text is already divided away from the raw data
	 * @param withimg
	 *            whether the raw data contains an image
	 * @throws TobytesException
	 *             when there are problems happening in this process
	 */
	public void fromFrontBytes(byte[] text, final String filepath1,
			final String filepath2, final boolean divide, boolean withimg,
			String... possurl) {
		
	}

	/**
	 * Calculate the rowkey used by flow count table.
	 * 
	 * @return the corresponding rowkey
	 */
	public byte[] calCounterRowkey() {
		final long ts = this.getLongValue(TRFieldEnum.TIMESTAMP);
		final SimpleDateFormat formattmp = new SimpleDateFormat("yyyyMMddHHmm");
		final String tsstr = formattmp.format(ts);
		int bidStr = getIntValue(TRFieldEnum.BAY_ID);
		return Bytes.toBytes(bidStr + "-" + tsstr);
	}

	public static void main(final String[] args) throws Exception {
		// Class.forName("cn.edu.sdu.cs.jnits.hbase.persistent.elike.TRValueEnumLike");

		TravelRecord tr = new TravelRecord();
		tr.setValue(TRFieldEnum.CAR_PLATE_NUMBER, "鲁AT1234");
		printTr(tr);
		tr.setValue(TRFieldEnum.CAR_PLATE_NUMBER, "鲁A1234警");
		printTr(tr);
		tr.setValue(TRFieldEnum.CAR_PLATE_NUMBER, "鲁A123警警");
		printTr(tr);

		tr.setValue(TRFieldEnum.BAY_ID, 200);
		tr.setValue(TRFieldEnum.CAMERA_LOCATION, 200);
		tr.setValue(TRFieldEnum.TIMESTAMP, System.currentTimeMillis());
		tr.setValue(TRFieldEnum.IMAGE_URLS, new String[] {"http://localhost:8080/test.jpg", null, null });
		printTr(tr);
		tr = TravelRecord.fromBytes(tr.toBytes());
		printTr(tr);
	}

	/**
	 * Print the content of an instance of this class.
	 * 
	 * @param tr the instance
	 */
	private static void printTr(final TravelRecord tr) {
		System.out.println(tr);
		final byte[] bs = tr.toBytes();
		final byte[] c = new byte[100];
		for (int i = 0; i < 50; i++)
			c[i] = (byte) i;
		printArray(bs);
		printArray(c);
	}

	/**
	 * Print the content of a byte array.
	 * 
	 * @param bs
	 *            the byte array
	 */
	private static void printArray(byte[] bs) {
		System.out.print("len= " + bs.length + ":\t[");
		for (byte b : bs)
			System.out.print(String.format("%5d", b));
		System.out.println("]");

	}
}