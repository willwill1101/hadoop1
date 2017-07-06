package com.ehl.itgs.interfaces.bean.analysis;

import java.util.ArrayList;

import com.ehl.itgs.interfaces.bean.ResultCar;

/** 
 *  伴随车辆查询及多点为分析比对等结果BEAN
 */

public class LocusCarResult {
	/**
	 * 车辆特征
	 */
	private  ResultCar car;
    
    /**
     * 车辆经过卡口信息， <卡口编号,经过时间,图片集合>
     */
    private ArrayList<PassTgsInfo> tgsInfo;

	
	/**
	 * 获取车辆特定信息
	 */
	public ResultCar getCar() {
		return car;
	}
	
	/**
	 * 设置车辆特征信息
	 */
	public void setCar(ResultCar car) {
		this.car = car;
	}

	/**
	 * 获取车辆经过卡口信息， <卡口编号,经过时间>
	 * @return the tgsInfo
	 */
	public ArrayList<PassTgsInfo> getTgsInfo() {
		return tgsInfo;
	}
	
	/**
	 * 设置车辆经过卡口信息， <卡口编号,经过时间>
	 * @param tgsInfo the tgsInfo to set
	 */
	public void setTgsInfo(ArrayList<PassTgsInfo> tgsInfo) {
		this.tgsInfo = tgsInfo;
	}
}
