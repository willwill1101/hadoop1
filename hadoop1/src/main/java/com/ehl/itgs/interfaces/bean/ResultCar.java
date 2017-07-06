package com.ehl.itgs.interfaces.bean;

import java.io.Serializable;

/** 
 *  车辆信息
 */

public class ResultCar implements Serializable{
	/**
	 * 
	 */
	private static final long serialVersionUID = 1806065301725442056L;

	/**
	 * 车辆品牌。
	 */
	private  String carBrand;
	
	/**
	 * 车身颜色
	 */
	private  String carColor;
	
	/**
	 * 号牌颜色
	 */
    private  String plateColor ;
    /**
	 * 车辆号牌号码
	 */
    private  String plateNumber ;
    /**
	 * 号牌种类
	 */
    private  String plateType ;
    /**
	 * 通过时间
	 */
    private  String timeStamp ;
   
    /**
	 * 通过卡口
	 */
    private  String tgs ;
    /**
	 * 通行图片地址
	 */
    private String[] imgUrls; 
    /**
	 * 车道编号 
	 */
    private  String landIe ;
    /**
     * 通行速度
     */
    private  String speed; 
    /**
     * 车行方向
     */
    private  int travelOrientation;
    
    
    private String carState; //车辆状态
    private String loncatonId; //地点编号
    private String captureDir; //抓拍方向
    
    
    /**
	 * 是否是主车,伴随车辆使用
	 */
	private boolean isMainCar;
	
	/**
	 * 行为描述
	 */
	private String content;
	/**
	 * 是否为主车,伴随车辆使用
	 * @return true：是 false：否
	 */
	public boolean isMainCar() {
		return isMainCar;
	}
	/**
	 * 设置车辆为主车,伴随车辆使用
	 * @param isMainCar
	 */
	public void setMainCar(boolean isMainCar) {
		this.isMainCar = isMainCar;
	}
	/**
	 * 获取车辆品牌
	 * @return the carBrand
	 */
	public String getCarBrand() {
		return carBrand;
	}
	/**
	 * 设置车辆品牌
	 * @param carBrand the carBrand to set
	 */
	public void setCarBrand(String carBrand) {
		this.carBrand = carBrand;
	}
	/**
	 * 获取车身颜色
	 * @return the carColor
	 */
	public String getCarColor() {
		return carColor;
	}
	/**
	 * 设置车身颜色
	 * @param carColor the carColor to set
	 */
	public void setCarColor(String carColor) {
		this.carColor = carColor;
	}
	
	/**
	 * 获取车牌颜色
	 * @return the plateColor
	 */
	public String getPlateColor() {
		return plateColor;
	}
	/**
	 * 设置车牌颜色
	 * @param plateColor the plateColor to set
	 */
	public void setPlateColor(String plateColor) {
		this.plateColor = plateColor;
	}
	/**
	 * 获取车牌号码，支持模糊
	 * @return the plateNumber
	 */
	public String getPlateNumber() {
		return plateNumber;
	}
	/**
	 * 设置车牌号码，支持模糊
	 * @param plateNumber the plateNumber to set
	 */
	public void setPlateNumber(String plateNumber) {
		this.plateNumber = plateNumber;
	}
	/**
	 * 获取号牌种类
	 * @return the plateType
	 */
	public String getPlateType() {
		return plateType;
	}
	/**
	 * 设置号牌种类
	 * @param plateType the plateType to set
	 */
	public void setPlateType(String plateType) {
		this.plateType = plateType;
	}
	/**
	 * 获取通过时间
	 * @return the timeStamp
	 */
	public String getTimeStamp() {
		return timeStamp;
	}
	/**
	 * 设置通过时间
	 * @param timeStamp the timeStamp to set
	 */
	public void setTimeStamp(String timeStamp) {
		this.timeStamp = timeStamp;
	}
	/**
	 * 获取卡口编号
	 * @return the tgs
	 */
	public String getTgs() {
		return tgs;
	}
	/**
	 * 设置卡口编号
	 * @param tgs the tgs to set
	 */
	public void setTgs(String tgs) {
		this.tgs = tgs;
	}
	/**
	 * 获取图片信息集合
	 * @return the imgUrls
	 */
	public String[] getImgUrls() {
		return imgUrls;
	}
	/**
	 * 设置图片信息集合
	 * @param imgUrls the imgUrls to set
	 */
	public void setImgUrls(String[] imgUrls) {
		this.imgUrls = imgUrls;
	}
	/**
	 * 获取车道编号
	 * @return the landIe
	 */
	public String getLandIe() {
		return landIe;
	}
	/**
	 * 设置车道编号
	 * @param landIe the landIe to set
	 */
	public void setLandIe(String landIe) {
		this.landIe = landIe;
	}
	/**
	 * 获取车速
	 * @return the speed
	 */
	public String getSpeed() {
		return speed;
	}
	/**
	 * 设置车速
	 * @param speed the speed to set
	 */
	public void setSpeed(String speed) {
		this.speed = speed;
	}
	/**
	 * 获取车行方向
	 * @return the travelOrientation
	 */
	public int getTravelOrientation() {
		return travelOrientation;
	}
	/**
	 * 设置车行方向
	 * @param travelOrientation the travelOrientation to set
	 */
	public void setTravelOrientation(int travelOrientation) {
		this.travelOrientation = travelOrientation;
	}
	/**
	 * 获取车辆状态
	 */
	public String getCarState() {
		return carState;
	}
	
	/**
	 * 设置车辆状态
	 */
	public void setCarState(String carState) {
		this.carState = carState;
	}
	
	/**
	 * 获取地点编号
	 */
	public String getLoncatonId() {
		return loncatonId;
	}
	
	/**
	 * 设置地点编号
	 */
	public void setLoncatonId(String loncatonId) {
		this.loncatonId = loncatonId;
	}
	
	/**
	 * 设置抓拍方向
	 */
	public String getCaptureDir() {
		return captureDir;
	}
	
	/**
	 * 设置抓拍方向
	 */
	public void setCaptureDir(String captureDir) {
		this.captureDir = captureDir;
	}
	/**
	 * 获取违法类型
	 */
	public String getContent() {
		return content;
	}
	/**
	 * 设置违法类型
	 */
	public void setContent(String content) {
		this.content = content;
	}
    
	public String toString(){
		return  plateNumber+";"+plateType+";"+carBrand+";"+carColor+";"+plateColor+";"+tgs
		+";"+timeStamp+";"+((imgUrls!=null&&imgUrls.length>0)?imgUrls[0]:"")+";"+speed+";"
		+landIe+";"+travelOrientation+";"+carState+";"+loncatonId+";"+captureDir;
	}
}
