package com.ehl.tvc.realtime.esbolt.dao;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;

import org.elasticsearch.action.index.IndexRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ehl.tvc.realtime.esbolt.bean.TrPlateEsBean;
import com.ehl.tvc.realtime.esbolt.util.CarPlateCommonUtil;
import com.ehl.tvc.realtime.esbolt.util.UUIDGenerator;
import com.google.gson.Gson;

public class TravelRecordESDao {
	private Logger log = LoggerFactory.getLogger(this.getClass());
	private Gson gson = new Gson();
	DateFormat df = new SimpleDateFormat("yyyyMM");
	private String esIndex = "ehlindex201501_06";
	private String esType = "pass_car";

	/**
	 * 保存过车记录
	 * 
	 * @param passCarBean
	 */
	public void saveTravelRecord(TrPlateEsBean passCarBean) {
		TrPlateEsBean esBean = passCarBean;
		if ("无牌".equals(esBean.getCar_plate_number())) {
			esBean.setCar_plate_number_index("无牌");
			esBean.setCar_plate_number_prefix("");
		} else {
			esBean.setCar_plate_number_index(getCarplateIndex(esBean.getCar_plate_number()));
			esBean.setCar_plate_number_prefix(getCarPlatePre(esBean.getCar_plate_number().toUpperCase()));
		}
		String  yyyyMM =df.format(new Date(passCarBean.getTimestamp()));
		int yyymmInt = Integer.valueOf(yyyyMM);
		if(yyymmInt<=201506){
			 esIndex = "ehlindex201501_06";
		}else if(yyymmInt>=201507&&yyymmInt<=201512){
			 esIndex = "ehlindex201507_12";
		}else if(yyymmInt>=201601&&yyymmInt<=201606){
			 esIndex = "ehlindex201601_06";
		}else if(yyymmInt>=201607&&yyymmInt<=201612){
			 esIndex = "ehlindex201607_12";
		}else if(yyymmInt>=201701&&yyymmInt<=201706){
			 esIndex = "ehlindex201701_06";
		}else if(yyymmInt>=201707&&yyymmInt<=201708){
			 esIndex = "ehlindex201707_08";
		}else if(yyymmInt>=201709&&yyymmInt<=201710){
			 esIndex = "ehlindex201709_10";
		}else if(yyymmInt>=201711&&yyymmInt<=201712){
			 esIndex = "ehlindex201711_12";
		}
		SearchUtils.getInstance().bulkProcessor(
				new IndexRequest(esIndex, esType, esBean.getId())
						.source(gson.toJson(esBean)));
	}

	private String getCarplateIndex(String carplate) {
		if (carplate == null || carplate.trim().equals("")) {
			return "";
		}
		try {
			return CarPlateCommonUtil.produceCarPlateIndexStr(carplate);
		} catch (Exception e) {
			log.error("获取车牌索引格式报错：" + carplate, e);
		}
		return "";
	}

	private String getCarPlatePre(String carPlate) {
		if (carPlate != null && !carPlate.trim().equals("")) {
			String hpslStr = CarPlateCommonUtil.getPrefixFromCarPlate(carPlate);
			return hpslStr;
		}
		return "";
	}

}
