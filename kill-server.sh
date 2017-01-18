#!/bin/bash
kill -9 `jps -m | grep $3 | grep $5 | grep -v grep | awk '{print $1}'`

export val="not"
while true; do
if [ "$val" == "" ]; then
  break
fi
sleep 1
export val=`jps -m | grep $3 | grep $5`
done

