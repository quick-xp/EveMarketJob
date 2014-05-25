EveMarketJob
======================
Hadoopを使用して、Eve-Central(http://eve-central.com)で出力されるマーケットデータを集計する
現在は各ソーラーシステム毎の品目別売上数、売上合計金額を出力する機能のみ実装しています。

使い方
------
使用するにあたりCDH4のインストールが必須となります。

実行方法については /lib/evemarketjob.shを参考にしてください。

#!/bin/bash

if [ $# -ne 1 ]; then
	echo "引数は1つ必要です"
	exit 1
fi

hadoop fs -rm -R /user/chato/job/out/CollectMarketOrder
hadoop fs -rm -R /user/chato/job/out/CollectLastReport
hadoop fs -rm -R /user/chato/job/out/AnalyzeSellOrderPriceInEachSystem
hadoop fs -rm -R /user/chato/job/out/AnalyzeSellOrderPriceInEachSystem2
./collect_market_order.sh $1 /user/chato/job/out/CollectMarketOrder
./collect_last_report.sh $1 /user/chato/job/out/CollectLastReport 
./margetime.sh /user/chato/job/out/CollectLastReport /user/chato/job/out/CollectLastReport
./analyze_sell_order_price_in_each_system.sh /user/chato/job/out/CollectMarketOrder /user/chato/job/out/CollectLastReport/lastReport.txt /user/chato/job/out/AnalyzeSellOrderPriceInEachSystem
./analyze_sell_order_price_in_each_system2.sh /user/chato/job/out/CollectMarketOrder /user/chato/job/out/CollectLastReport/lastReport.txt /user/chato/job/out/AnalyzeSellOrderPriceInEachSystem2
./margeMarketData.sh /user/chato/job/out/AnalyzeSellOrderPriceInEachSystem sell_order_price_in_each_system.csv
./margeMarketData.sh /user/chato/job/out/AnalyzeSellOrderPriceInEachSystem2 sell_order_price_in_each_system2.csv
rm ./sell_order_price_in_each_system.csv
hadoop fs -get /user/chato/job/out/AnalyzeSellOrderPriceInEachSystem/sell_order_price_in_each_system.csv 
rm ./sell_order_price_in_each_system2.csv
hadoop fs -get /user/chato/job/out/AnalyzeSellOrderPriceInEachSystem2/sell_order_price_in_each_system2.csv

