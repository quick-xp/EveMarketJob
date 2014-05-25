package eve.market.hadoop.job;

import org.apache.hadoop.conf.*;
import org.apache.hadoop.util.*;

public class EveMarketJob {

	public static void main(String[] args) throws Exception {
		int res = 1;
		if (args.length < 3) {
			if (args.length == 1) {
				if (args[0].equals("-l")) {
					System.out.println("ジョブ一覧");
					System.out
							.println("CollectMarketOrder : 引数0=CollectMarketJob,引数1=Input,引数2=Output");
					System.out
							.println("CollectLastReport : 引数0=CollectLastReport,引数1=Input,引数2=Output");
					System.out
							.println("AnalyzeSellOrderPriceInEachSystem : 引数0=AnalyzeSellOrderPriceInEachSystem,引数1=Input,引数2=lastreported,引数3=Output");
					System.out
							.println("AnalyzeSellOrderPriceInEachSystem2 : 引数0=AnalyzeSellOrderPriceInEachSystem,引数1=Input,引数2=lastreported,引数3=Output");
				}
			} else {
				System.out.println("引数が不足しています。");
				System.out.println("ジョブ一覧を表示する場合は引数に -l を入力してください");
			}
		} else {
			if (args[0].equals("CollectMarketOrder")) {
				res = ToolRunner.run(new Configuration(),
						new CollectMarketOrder(), args);
			}
			if (args[0].equals("CollectLastReport")) {
				res = ToolRunner.run(new Configuration(),
						new CollectLastReport(), args);
			}
			if (args[0].equals("AnalyzeSellOrderPriceInEachSystem")) {
				res = ToolRunner.run(new Configuration(),
						new AnalyzeSellOrderPriceInEachSystem(), args);
			}
			if (args[0].equals("AnalyzeSellOrderPriceInEachSystem2")) {
				res = ToolRunner.run(new Configuration(),
						new AnalyzeSellOrderPriceInEachSystem2(), args);
			}
		}

		System.exit(res);
	}

}
