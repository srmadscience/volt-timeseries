#!/bin/sh

duration=300
DBHOST=$1

for datasize in 126 32767 2147483647
do
	for changeinterval in 1 10 100 1000
	do
		for timechangeinterval in 1 10 100 1000
		do

		java -jar ../jars/voltdb-timeseries-runnable.jar ${DBHOST} ds${datasize}changeint${changeinterval}timeint${timechangeinterval}    100 ${duration} ${datasize}  ${changeinterval}  1 ${timechangeinterval}
		sleep 60

		done
	done
done
