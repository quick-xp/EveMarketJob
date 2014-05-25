#!/bin/bash

if [ $# -ne 3 ]; then
	echo "引数は3つ必要です"
	exit 1
fi

hadoop jar EveMarketJob.jar eve.market.hadoop.job.EveMarketJob AnalyzeSellOrderPriceInEachSystem $1 $2 $3
