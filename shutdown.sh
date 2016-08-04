#!/bin/sh
pidlist=`ps -ef|grep uncode-mq|grep -v "grep"|awk '{print $2}'`
if [ "$pidlist" = "" ]
   then
       echo "no umq pid alive!"
else
  echo "umq id list :$pidlist"
  kill -9 $pidlist
  echo "kill $pidlist"
  echo "umq stop success"
fi
