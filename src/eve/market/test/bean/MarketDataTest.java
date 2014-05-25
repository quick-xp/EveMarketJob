package eve.market.test.bean;

import static org.junit.Assert.*;

import java.util.ArrayList;

import org.junit.Test;

import eve.market.hadoop.bean.MarketData;
import eve.market.hadoop.tool.MarketDataTool;

public class MarketDataTest {

	@Test
	public void setReportedtimeTest001() {
		MarketData marketData = new MarketData();
		marketData.setReportedtime("\"2014-02-03 13:05:59.204354\"");
		if (marketData.getReportedtime() == null){
			 fail();
		}
		
	}

	@Test
	public void sortByReportTimeDescTest001() {
		MarketData marketData = new MarketData();
		MarketData marketData2 = new MarketData();
		MarketData marketData3 = new MarketData();
		
		marketData.setReportedtime("2014-02-03 13:06:02.036339");
		marketData2.setReportedtime("2014-02-03 13:06:04.036339");
		marketData3.setReportedtime("2014-02-03 13:06:03.936339");
		
		ArrayList<MarketData> list = new ArrayList<MarketData>();
		list.add(marketData);
		list.add(marketData2);
		list.add(marketData3);
		MarketDataTool tool = new MarketDataTool();
		tool.sortByReportTimeDesc(list);
		System.out.println("sortByReportTimeDescTest001 :" + list.get(0).getReportedtimeStr());
		
	}
	
	@Test
	public void filterMarketDataByReportedTimeTest001() {
		MarketData marketData = new MarketData();
		MarketData marketData2 = new MarketData();
		MarketData marketData3 = new MarketData();
		
		marketData.setReportedtime("2014-02-03 13:06:02.036339");
		marketData.setData("aaa");
		marketData2.setReportedtime("2014-02-03 13:06:04.036339");
		marketData2.setData("bbb");
		marketData3.setReportedtime("2014-02-03 13:06:03.036339");
		marketData3.setData("ccc");
		
		ArrayList<MarketData> list = new ArrayList<MarketData>();
		list.add(marketData);
		list.add(marketData2);
		list.add(marketData3);
		MarketDataTool tool = new MarketDataTool();
		String result = tool.filterMarketDataByReportedTime(list);
		System.out.println(result);
			
	}
}
