#!/bin/bash

if [ $# -ne 2 ]; then
	echo "引数は2つ必要です"
	exit 1
fi

hadoop fs -text $1/part-* | hadoop fs -put - $1/$2

