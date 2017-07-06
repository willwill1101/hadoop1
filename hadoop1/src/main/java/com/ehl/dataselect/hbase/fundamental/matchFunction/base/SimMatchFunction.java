package com.ehl.dataselect.hbase.fundamental.matchFunction.base;

import com.ehl.dataselect.hbase.persistent.TRFieldBase;
import com.ehl.dataselect.hbase.persistent.TravelRecord;

/**
 * The first kind of MatchFunction, which corresponds to single logical expression.
 * 
 * @author SDU.lwlin
 *
 */
public class SimMatchFunction extends MatchFunction
{
	private int level;
	private OP op = null;				//比较字符
	private TRFieldBase field = null;	//比较属性
	private Object value_1 = null;		//查询条件1
	private Object value_2 = null;		//查询条件2,如果有

	public SimMatchFunction()
	{
	}
	
	public SimMatchFunction(int level, OP op, TRFieldBase field, Object value_1, Object value_2)
	{
		this.level = level;
		this.op = op;
		this.field = field;
		this.value_1 = value_1;
		this.value_2 = value_2;
	}

	/**
	 * Get the level of this MatchFunction.
	 * 
	 * @return
	 */
	public int getLevel()
	{
		return level;
	}

	/**
	 * Get the operator of this MatchFunction. <br>
	 * 
	 * @return
	 * 		{@link MatchFunction.OP#MATCH}, {@link MatchFunction.OP#IS} or {@link MatchFunction.OP#BETWEEN}
	 */
	public OP getOp()
	{
		return op;
	}

	/**
	 * Get the field to be operated on of this MatchFunction.
	 * 
	 * 
	 * @return
	 */
	public TRFieldBase getField()
	{
		return field;
	}

	/**
	 * Get the first value.
	 * 
	 * @return
	 */
	public Object getValue_1()
	{
		return value_1;
	}

	/**
	 * Get the second value.
	 * 
	 * @return
	 */
	public Object getValue_2()
	{
		return value_2;
	}

	@Override
	public String toString()
	{
		StringBuffer sb = new StringBuffer();
		for (int i = 1; i < level; i++)
			sb.append("-\t");
		sb.append(op);
		sb.append('\t');
		sb.append(field.NAME);
		sb.append('\t');
		String[] vs = field.printToString(value_1, value_2);
		sb.append(vs[0]);
		if (vs.length > 1 && vs[1] != null)
		{
			sb.append('\t');
			sb.append(vs[1]);
		}
		return sb.toString();
	}
	/**
	 * 比较记录与查询条件子项，比较信息在TRFieldEnum中
	 */
	@Override
	public boolean match(byte[] bs)
	{
		return TravelRecord.match(bs, field, value_1, value_2);
	}
}