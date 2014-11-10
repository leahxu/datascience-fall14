#rm -rf ~/hadoop_apps/BigramGenerator/bigram
#javac -Xlint -classpath ${HADOOP_HOME}/share/hadoop/common/hadoop-common-2.3.0-cdh5.1.2.jar:${HADOOP_HOME}/share/hadoop/mapreduce/hadoop-mapreduce-client-core-2.3.0-cdh5.1.2.jar:/home/leahxu/cloudera/cdh5.1/hadoop-2.3.0-cdh5.1.2/share/hadoop/annotations/hadoop-annotations-2.0.0-cdh4.0.1.jar -d ~/hadoop_apps/BigramGenerator BigramGenerator.java
#jar cvf BigramGenerator.jar -C ~/hadoop_apps/BigramGenerator/ bigram
#hadoop fs -rm -r -f /bigram/tmp
#hadoop jar BigramGenerator.jar bigram.BigramGenerator /bigram/input/bible+shakes.nopunc /bigram/tmp

rm -rf ~/hadoop_apps/BigramTopFive/bigram
javac -Xlint -classpath ${HADOOP_HOME}/share/hadoop/common/hadoop-common-2.3.0-cdh5.1.2.jar:${HADOOP_HOME}/share/hadoop/mapreduce/hadoop-mapreduce-client-core-2.3.0-cdh5.1.2.jar:/home/leahxu/cloudera/cdh5.1/hadoop-2.3.0-cdh5.1.2/share/hadoop/annotations/hadoop-annotations-2.0.0-cdh4.0.1.jar -d ~/hadoop_apps/BigramTopFive BigramTopFive.java
jar cvf BigramTopFive.jar -C ~/hadoop_apps/BigramTopFive/ bigram
hadoop fs -rm -r -f /bigram/result
hadoop jar BigramTopFive.jar bigram.BigramTopFive /bigram/tmp/part-00000 /bigram/result


