package eve.market.hadoop.job;

import java.io.*;
import java.util.*;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.*;

import eve.market.hadoop.bean.MarketData;
import eve.market.hadoop.tool.MarketDataTool;

/***
 * EveMarketDataを読み込み、orderid,stationidをキーとしレコードを複数行から1行にまとめる
 * その際、レコードは最新と最古レコードのみにフィルターする
 * 
 * @author chato
 */
public class CollectMarketOrder extends Configured implements Tool {

	public static class Map extends MapReduceBase implements
			Mapper<LongWritable, Text, Text, Text> {

		private Text lineData = new Text();
		private Text keyData = new Text();

		public void map(LongWritable key, Text value,
				OutputCollector<Text, Text> output, Reporter reporter)
				throws IOException {
			StringTokenizer itr = new StringTokenizer(value.toString(), "\r\n");
			while (itr.hasMoreTokens()) {
				lineData.set(itr.nextToken());
				// key抽出 0:orderid,3:stationid
				String[] keyList = lineData.toString().split(",");
				keyData.set(keyList[0] + "," + keyList[3]);
				output.collect(keyData, lineData);
			}
		}
	}

	public static class Reduce extends MapReduceBase implements
			Reducer<Text, Text, Text, Text> {
		private MarketDataTool marketDataTool = new MarketDataTool();

		public void reduce(Text key, Iterator<Text> values,
				OutputCollector<Text, Text> output, Reporter reporter)
				throws IOException {
			String v = "";
			ArrayList<MarketData> marketDataLists = new ArrayList<MarketData>();
			// valueを取り出す
			while (values.hasNext()) {
				String value = values.next().toString();
				// start issuedに,が含まれる対応
				value = marketDataTool.removeStrForCSV(value);
				// end issuedに,が含まれる対応
				String[] keyList = value.toString().split(",");
				MarketData marketData = new MarketData();
				marketData.setData(value);
				// 14:reportedtime
				marketData.setReportedtime(keyList[14]);
				marketDataLists.add(marketData);
			}
			// マーケットデータを最新日付と最古日付のみにフィルターし、Stringデータにして返却する
			v = marketDataTool.filterMarketDataByReportedTime(marketDataLists);

			output.collect(new Text(v), new Text(""));
		}
	}

	public int run(String[] args) throws Exception {
		JobConf conf = new JobConf(getConf(), CollectMarketOrder.class);
		conf.setJobName("CollectMarketOrder");

		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(Text.class);

		conf.setMapperClass(Map.class);
		conf.setReducerClass(Reduce.class);

		conf.setInputFormat(TextInputFormat.class);
		conf.setOutputFormat(TextOutputFormat.class);

		List<String> other_args = new ArrayList<String>();
		for (int i = 0; i < args.length; ++i) {
			other_args.add(args[i]);
		}

		FileInputFormat.setInputPaths(conf, new Path(other_args.get(1)));
		FileOutputFormat.setOutputPath(conf, new Path(other_args.get(2)));

		JobClient.runJob(conf);
		return 0;
	}

}
