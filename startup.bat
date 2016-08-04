@echo off
::java -jar lib/uncode-mq-1.0.0.jar -cfg conf/config.properties > logs/test.log &
java -jar -Xms512m -Xms1024m lib/uncode-mq-1.0.0.jar -cfg conf/config.properties
@pause