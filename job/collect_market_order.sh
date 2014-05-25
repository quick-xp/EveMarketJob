#!/bin/bash

if [ $# -ne 2 ]; then
	echo "引数は2つ必要です"
	exit 1
fi

hadoop jar EveMarketJob.jar eve.market.hadoop.job.EveMarketJob CollectMarketOrder $1 $2
