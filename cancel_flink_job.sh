#!/bin/bash

let LINES=$(flink list -r | wc -l)
echo $LINES
JOBS=`expr $LINES - 3`
echo $JOBS

for ((n=0;n<$JOBS;n++))
do
#echo hello
FLINK_JOB=$(flink list | sed -n '3 p' | cut -c23-55)
flink cancel $FLINK_JOB
done
