package eve.market.hadoop.tool;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;

import eve.market.hadoop.bean.MarketData;

public class MarketDataTool {
	/**
	 * マーケットデータを日付順(降順)にソートする
	 * @param list
	 */
	public void sortByReportTimeDesc(ArrayList<MarketData> list){
		Collections.sort(list, new Comparator<MarketData>() {
			public int compare(MarketData o1, MarketData o2) {
				return o1.getReportedtime().compareTo(o2.getReportedtime())
						* (-1);
			}
		});
	}
	/**
	 * マーケットデータを最新日付と最古日付のみにフィルターし、Stringデータにして返却する
	 */
	public String filterMarketDataByReportedTime(ArrayList<MarketData> list){
		int count = list.size();
		if(1 < count){
			sortByReportTimeDesc(list);
			return list.get(0).getData() + ";" + list.get(count-1).getData();
		}else if(count == 1){
			return list.get(0).getData();
		}
		return "";
	}
	
	/**
	 * マーケットデータ1行からCSV解析に不要な値を取り除く
	 */
	public String removeStrForCSV(String str){
		str = str.replaceAll("days, 0:00:00", "");
		str = str.replaceAll("day, 0:00:00","");
		return str;
	}
	/**
	 * 最終報告日付を返却する
	 * @param marketDataLists
	 * @return 最終報告日付
	 */
	public String getLastReportedTime(ArrayList<MarketData> marketDataLists) {
		sortByReportTimeDesc(marketDataLists);
		return marketDataLists.get(0).getReportedtimeStr();
	}
}
