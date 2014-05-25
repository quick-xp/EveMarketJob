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
 * EveMarketDataを読み込み、stationidをキーとし、各ステーションの最終報告日付を求める
 * 
 * @author chato
 */
public class CollectLastReport extends Configured implements Tool {

	public static class Map extends MapReduceBase implements
			Mapper<LongWritable, Text, Text, Text> {

		private Text valueData = new Text();
		private Text keyData = new Text();
		private MarketDataTool marketDataTool = new MarketDataTool();

		public void map(LongWritable key, Text value,
				OutputCollector<Text, Text> output, Reporter reporter)
				throws IOException {
			StringTokenizer itr = new StringTokenizer(value.toString(), "\r\n");
			while (itr.hasMoreTokens()) {
				// 0:stationid,14reportedtime
				String[] keyList = marketDataTool.removeStrForCSV(
						itr.nextToken()).split(",");
				keyData.set(keyList[3]);
				valueData.set(keyList[14]);
				output.collect(keyData, valueData);
			}
		}
	}

	public static class Reduce extends MapReduceBase implements
			Reducer<Text, Text, Text, Text> {
		private MarketDataTool marketDataTool = new MarketDataTool();
		private Text valueData = new Text();

		public void reduce(Text key, Iterator<Text> values,
				OutputCollector<Text, Text> output, Reporter reporter)
				throws IOException {
			String v = "";
			ArrayList<MarketData> marketDataLists = new ArrayList<MarketData>();
			// value(ReportedTime)を取り出す
			while (values.hasNext()) {
				String value = values.next().toString();
				MarketData marketData = new MarketData();
				marketData.setReportedtime(value);
				marketDataLists.add(marketData);
			}
			// ステーションの最終報告日付を取得する
			try {
				v = marketDataTool.getLastReportedTime(marketDataLists);
				valueData.set(v);
				output.collect(key, valueData);
			} catch (Exception e) {
				;
			}
		}
	}

	public int run(String[] args) throws Exception {
		JobConf conf = new JobConf(getConf(), CollectLastReport.class);
		conf.setJobName("CollectLastReport");

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
