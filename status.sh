#!/bin/sh
pidlist=`ps -ef|grep uncode-mq|grep -v "grep"|awk '{print $2}'`
if [ "$pidlist" = "" ]
   then
       echo "umq stop"
else
  echo "umq running, id :$pidlist"
fi
