package eve.market.hadoop.bean;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

public class MarketData {
	private String orderid;
	private String regionid;
	private String systemid;
	private String stationid;
	private String typeid;
	private String bid;
	private Double price;
	private String minvolume;
	private Integer volremain;
	private String volenter;
	private Date issued;
	private String duration;
	private String range;
	private String reportedby;
	private Date reportedtime;
	private String data;

	public MarketData(String data) {
		String[] splitValue = data.split(",");
		this.setOrderid(splitValue[0]);
		this.setRegionid(splitValue[1]);
		this.setSystemid(splitValue[2]);
		this.setStationid(splitValue[3]);
		this.setTypeid(splitValue[4]);
		this.setBid(splitValue[5]);
		this.setPrice(splitValue[6]);
		this.setMinvolume(splitValue[7]);
		this.setVolremain(splitValue[8]);
		this.setVolenter(splitValue[9]);
		this.setIssued(splitValue[10]);
		this.setDuration(splitValue[11]);
		this.setRange(splitValue[12]);
		this.setReportedby(splitValue[13]);
		this.setReportedtime(splitValue[14]);
	}

	public MarketData(){
		;
	}
	public String getOrderid() {
		return orderid;
	}

	public void setOrderid(String orderid) {
		this.orderid = orderid.replaceAll("\"", "");
	}

	public String getRegionid() {
		return regionid;
	}

	public void setRegionid(String regionid) {
		this.regionid = regionid.replaceAll("\"", "");
	}

	public String getSystemid() {
		return systemid;
	}

	public void setSystemid(String systemid) {
		this.systemid = systemid.replaceAll("\"", "");
	}

	public String getStationid() {
		return stationid;
	}

	public void setStationid(String stationid) {
		this.stationid = stationid.replaceAll("\"", "");
	}

	public String getTypeid() {
		return typeid;
	}

	public void setTypeid(String typeid) {
		this.typeid = typeid.replaceAll("\"", "");
	}

	public String getBid() {
		return bid;
	}

	public void setBid(String bid) {
		this.bid = bid.replaceAll("\"", "");
	}

	public Double getPrice() {
		return price;
	}

	public void setPrice(String price) {
		this.price = Double.parseDouble(price.replaceAll("\"", ""));
	}

	public String getMinvolume() {
		return minvolume;
	}

	public void setMinvolume(String minvolume) {
		this.minvolume = minvolume.replaceAll("\"", "");
	}

	public Integer getVolremain() {
		return volremain;
	}

	public void setVolremain(String volremain) {
		this.volremain = Integer.parseInt(volremain.replaceAll("\"", ""));
	}

	public String getVolenter() {
		return volenter;
	}

	public void setVolenter(String volenter) {
		this.volenter = volenter.replaceAll("\"", "");
	}

	public Date getIssued() {
		return issued;
	}

	public void setIssued(String issued) {
		String pattern = "yyyy-MM-dd HH:mm:ss";
		SimpleDateFormat sdf = new SimpleDateFormat(pattern);
		try {
			this.issued = sdf.parse((issued.replaceAll("\"", "")));
		} catch (ParseException e) {
			;
		}
	}

	public String getDuration() {
		return duration;
	}

	public void setDuration(String duration) {
		this.duration = duration.replaceAll("\"", "").replace(" ", "");
	}

	public String getRange() {
		return range;
	}

	public void setRange(String range) {
		this.range = range.replaceAll("\"", "");
	}

	public String getReportedby() {
		return reportedby;
	}

	public void setReportedby(String reportedby) {
		this.reportedby = reportedby.replaceAll("\"", "");
	}

	public Date getReportedtime() {
		return reportedtime;
	}

	public String getReportedtimeStr() {
		String pattern = "yyyy-MM-dd HH:mm:ss";
		SimpleDateFormat sdf = new SimpleDateFormat(pattern);
		return sdf.format(this.reportedtime);
	}

	public void setReportedtime(Date reportedtime) {
		this.reportedtime = reportedtime;
	}

	public void setReportedtime(String reportedtime) {
		String pattern = "yyyy-MM-dd HH:mm:ss";
		SimpleDateFormat sdf = new SimpleDateFormat(pattern);
		try {
			String r = reportedtime.replaceAll("\"", "");
			String r2 = r.replaceAll("\\.[0-9]*", "");
			
			this.reportedtime = sdf.parse(r2);
		} catch (ParseException e) {
			;
		}
	}

	public String getData() {
		return data;
	}

	public void setData(String data) {
		this.data = data;
	}

}
