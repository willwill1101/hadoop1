/**
 * 
 */
package com.ehl.itgs.interfaces.bean.analysis;

/** 
 * 通过卡口信息
 */

public class PassTgsInfo {
	private String tgsId; //卡口ID
	private String passTime;  //经过时间
	private String imgUrl[]; //图片信息
	private String speed;    //车速
	private String travelOrientation;  //车行方向
	/**
	 * 获取卡口ID
	 */
	public String getTgsId() {
		return tgsId;
	}
	/**
	 * 设置卡口ID
	 * @param tgsId
	 */
	public void setTgsId(String tgsId) {
		this.tgsId = tgsId;
	}
	/**
	 * 获取经过时间
	 */
	public String getPassTime() {
		return passTime;
	}
	/**
	 * 设置经过时间
	 * @param passTime
	 */
	public void setPassTime(String passTime) {
		this.passTime = passTime;
	}
	/**
	 * 获取图片URL集合
	 */
	public String[] getImgUrl() {
		return imgUrl;
	}
	/**
	 * 设置图片URL集合
	 * @param imgUrl
	 */
	public void setImgUrl(String[] imgUrl) {
		this.imgUrl = imgUrl;
	}
	
	/**
	 * 获取经过车速
	 */
	public String getSpeed() {
		return speed;
	}
	
	/**
	 * 设置测速
	 */
	public void setSpeed(String speed) {
		this.speed = speed;
	}
	/**
	 * 获取车行方向
	 */
	public String getTravelOrientation() {
		return travelOrientation;
	}
	/**
	 * 设置车行方向
	 */
	public void setTravelOrientation(String travelOrientation) {
		this.travelOrientation = travelOrientation;
	}
	
	
}
