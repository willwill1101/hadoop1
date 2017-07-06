package com.ehl.dataselect.hbase.persistent;

import com.ehl.dataselect.hbase.fundamental.matchFunction.base.SimMatchFunction;

public abstract class TRFieldBase {

	private static int FIXED_SIZE_TALLY = 54;//属性长度和
	public final String NAME;			//属性名称
	public final int FIXED_SIZE;		//字符长度
	public int OFFSET;					//偏移量
	public boolean IS_ENUM_LIKE = false;
	
	
	public TRFieldBase(String name, int fixSize,int OFFSET) {
		this.NAME = name;
		this.FIXED_SIZE = fixSize;
		this.OFFSET = OFFSET;
	}

	public TRFieldBase(String name, int fixSize, boolean elike,int OFFSET) {
		this.NAME = name;
		this.FIXED_SIZE = fixSize;
		this.IS_ENUM_LIKE = elike;
		this.OFFSET = OFFSET;
	}

	/**
	 * Used for {@link TravelRecord#toString()}.
	 * 
	 * @param value the object
	 * @return the corresponding string value
	 */
	public String toTRtoString(Object value) {
		StringBuffer sb = new StringBuffer(this.toString());
		sb.append(" = ");
		sb.append(value);
		return sb.toString();
	};

	/**
	 * From internal presentation (String, Int, Long ...) to String, for
	 * MatchFunction 调试使用方法.
	 * 打印比较条件
	 * @param value_1 the first value
	 * @param value_2 the second value
	 * @return the corresponding string array
	 */
	public String[] printToString(Object value_1, Object value_2){
		return new String[] { value_1.toString() };
	}

	/**
	 * From String to internal presentation, used by MatchFunction parser.
	 * 使用比较条件构造MactchFunction单元
	 * @param level the MachFunction level
	 * @param str_1 the first value
	 * @param str_2 the second value
	 * @return the corresponding MatchFunction
	 * @throws IllegalArgumentException
	 */
	public abstract SimMatchFunction parseFromString(int level, String str_1,
			String str_2) throws IllegalArgumentException;

	/**
	 * Used by MatchFunction comparison at RegionServer.
	 * 比较
	 * @param bs the byte array
	 * @param value_1 the first value
	 * @param value_2 the second value
	 * @return {@code true} if the field matches the values, {@code false} otherwise.
	 */
	public abstract boolean match(byte[] bs, Object value_1, Object value_2);

	/**
	 * Fill the value of this field into the byte array at the appropriate position.
	 * 填充
	 * @param bs the byte array
	 * @param obj the value of this field
	 */
	public abstract void fillBytes(byte[] bs, Object obj);

	/**
	 * Read the value of this field from the byte array.
	 * 读取
	 * @param bs the byte array
	 * @return the corresponding value
	 */
	public abstract  Object readBytes(byte[] bs);
	
	/**
	 * Arrange the position of each field in the byte array, and at the same time calculate<br>
	 * the total size of all fixed fields.
	 * 获取各属性长度和，并计算各属性偏移量
	 * @return the total fixed size
	 */
	public static int getFixedSize() {
		return FIXED_SIZE_TALLY;
	}
	
}
