#!/bin/sh
java -jar -Xms512m -Xmx1024m lib/uncode-mq-1.0.0.jar -cfg conf/config.properties > logs/umq.log &
pidlist=`ps -ef|grep uncode-mq|grep -v "grep"|awk '{print $2}'`
if [ "$pidlist" = "" ]
   then
       echo "umq start faile!"
else
  echo "umq id list :$pidlist"
  echo "umq start success"
fi
