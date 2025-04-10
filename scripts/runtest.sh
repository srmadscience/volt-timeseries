#!/bin/sh

duration=180
DBHOST=$1

for datasize in  2147483647 3147483647 63 126 32767  65500 
do
	for changeinterval in 1 10 100 1000
	do
		for timechangeinterval in 1 10 100 1000
		do

				java -jar ../jars/voltdb-timeseries-runnable.jar ${DBHOST} ds${datasize}changeint${changeinterval}timeint${timechangeinterval}${compressYN}    100 ${duration} ${datasize}  ${changeinterval}  1 ${timechangeinterval} 
			sleep 10

		done
	done
done
