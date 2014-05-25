package eve.market.hadoop.job;

import java.io.*;
import java.net.URI;
import java.text.SimpleDateFormat;
import java.util.*;

import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.*;

import eve.market.hadoop.bean.MarketData;

/***
 * EveMarketDataを読み込み、Regionid,Systemid,typeIdをキーとし、各システム毎の品目別売上を求める
 * 
 * @author chato
 */
public class AnalyzeSellOrderPriceInEachSystem2 extends Configured implements
		Tool {

	public static class Map extends MapReduceBase implements
			Mapper<LongWritable, Text, Text, Text> {

		private Text valueData = new Text();
		private Text keyData = new Text();
		private HashMap<String, String> cachedData = new HashMap<String, String>();
		private String pattern = "yyyy-MM-dd HH:mm:ss";
		private SimpleDateFormat sdf = new SimpleDateFormat(pattern);

		/**
		 * 準備(最終報告日付を取得する)
		 * 
		 * @param job
		 */
		public void configure(JobConf job) {
			try {
				Path[] lastReportFiles = DistributedCache
						.getLocalCacheFiles(job);
				BufferedReader fis = new BufferedReader(new FileReader(
						lastReportFiles[0].toString()));
				String report;
				try {
					while ((report = fis.readLine()) != null) {
						String[] s = report.split("\t");
						cachedData.put(s[0].replaceAll("\"", ""), s[1]);
					}
				} finally {
					fis.close();
				}
			} catch (Exception e) {
				// throw e;
			}
		}

		public void map(LongWritable key, Text value,
				OutputCollector<Text, Text> output, Reporter reporter)
				throws IOException {
			StringTokenizer itr = new StringTokenizer(value.toString(), "\r\n");
			while (itr.hasMoreTokens()) {
				// 最新と最古を分割する
				String[] data = itr.nextToken().split(";");
				MarketData newest;
				MarketData oldest = null;
				newest = new MarketData(data[0]);
				if (data.length == 2) {
					oldest = new MarketData(data[1]);
				}
				// bid=0:sellorder
				if (newest.getBid().equals("0")) {
					// キー抽出
					keyData.set(newest.getRegionid() + ","
							+ newest.getSystemid() + "," + newest.getTypeid());

					// 売れた数を求める
					int sellCount = 0;
					Double sellPrice = 0.0;

					if (oldest != null) {
						sellCount = oldest.getVolremain()
								- newest.getVolremain();
						// 最新価格基準
						sellPrice = newest.getPrice() * sellCount;
					}

					// 最終報告時刻と比較し、最終報告時刻と最新データが一致しない場合、有効期限切れまたは完売の判断をする
					Date stationLastReport = null;
					try {
						stationLastReport = sdf.parse(cachedData.get(newest
								.getStationid()));
					} catch (Exception e) {
						System.err.print("error stationid:"
								+ newest.getStationid());
					}
					// 120分は誤差とする
					if (newest.getReportedtime().getTime() < stationLastReport
							.getTime() - 2 * 60 * 1000 * 60) {
						// 有効期限
						long expire = newest.getIssued().getTime()
								+ (Long.parseLong(newest.getDuration()) * 24 * 60 * 1000 * 60);
						// 有効期限切れ判定(最新のマーケットデータが有効期限から６時間未満の報告の場合、有効期限切れと判断する)
						if (newest.getReportedtime().getTime() + 6 * 60 * 1000
								* 60 > expire) {
							System.out.println("order_expired : "
									+ newest.getOrderid() + " expire: "
									+ expire + " reporttime: "
									+ newest.getReportedtime().getTime()
									+ " issued: "
									+ newest.getIssued().getTime()
									+ " duration: " + newest.getDuration()
									+ " tmp"
									+ Long.parseLong(newest.getDuration()) * 24
									* 60 * 1000 * 60);
						} else {
							// 有効期限切れでない場合完売と判断する(ただし計上しない)
							//sellCount += newest.getVolremain();
							//sellPrice += newest.getPrice() * sellCount;
							System.out.println("order_sold_out but not count : "
									+ newest.getOrderid());
						}
					}else{
						System.out.println("order_not_sold_out : "
								+ newest.getOrderid());
					}

					valueData.set(sellCount + "," + sellPrice);
					output.collect(keyData, valueData);
				}
			}
		}
	}

	public static class Reduce extends MapReduceBase implements
			Reducer<Text, Text, Text, Text> {
		private Text valueData = new Text();

		public void reduce(Text key, Iterator<Text> values,
				OutputCollector<Text, Text> output, Reporter reporter)
				throws IOException {
			long sellPrice = 0;
			Long sellCount = (long) 0;
			// value(ReportedTime)を取り出す
			while (values.hasNext()) {
				String value = values.next().toString();
				// 0:sellCount,1:sellPrice
				String[] sellValues = value.split(",");
				sellCount += Long.parseLong(sellValues[0]);
				sellPrice += Double.parseDouble(sellValues[1]);
			}
			valueData.set(sellCount.toString() + "," + sellPrice);
			output.collect(key, valueData);
		}
	}

	public int run(String[] args) throws Exception {
		JobConf conf = new JobConf(getConf(), CollectLastReport.class);
		conf.setJobName("AnalyzeSellOrderPriceInEachSystem2");

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

		DistributedCache.addCacheFile(new URI(other_args.get(2)), conf);
		FileInputFormat.setInputPaths(conf, new Path(other_args.get(1)));
		FileOutputFormat.setOutputPath(conf, new Path(other_args.get(3)));

		JobClient.runJob(conf);
		return 0;
	}

}