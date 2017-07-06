package com.ehl.dataselect.hbase.fundamental.matchFunction.base;

/**
 * The second kind of MatchFunction, which corresponds to more complicated compound logical expressions.
 * 
 * @author SDU.lwlin
 *
 *
 */
public class ComMatchFunction extends MatchFunction
{
	private int level;
	private OP op = null;
	private MatchFunction[] mfs = null;

	public ComMatchFunction()
	{
	}

	public ComMatchFunction(int level, OP op, MatchFunction[] mfs)
	{
		super();
		this.level = level;
		this.op = op;
		this.mfs = mfs;
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
	 * Get the operator of this MatchFunction.<br>
	 * 
	 * @return
	 * 		{@link MatchFunction.OP#AND} or {@link MatchFunction.OP#OR}
	 */
	public OP getOp()
	{
		return op;
	}

	/**
	 * Get all the sub-MatchFunctions.
	 * 
	 * @return
	 */
	public MatchFunction[] getMfs()
	{
		return mfs;
	}

	@Override
	public String toString()
	{
		StringBuffer sb = new StringBuffer();
		for (int i = 1; i < level; i++)
			sb.append("-\t");
		sb.append(op);
		sb.append('\n');
		for (MatchFunction mf : mfs)
		{
			sb.append(mf);
			if (mf.getClass() != ComMatchFunction.class)
				sb.append("\n");
		}
		return sb.toString();
	}
	/**
	 * 复合比较，AND，和OR的不同情况
	 */
	@Override
	public boolean match(byte[] bs)
	{
		switch (op)
		{
			case AND:
				for (MatchFunction mf : mfs)
				{
					if (mf.match(bs) == false)
						return false;
				}
				return true;
			case OR:
				for (MatchFunction mf : mfs)
				{
					if (mf.match(bs) == true)
						return true;
				}
				return false;
			default:
				return false;
		}
	}
}