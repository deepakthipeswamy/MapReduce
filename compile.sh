$ mkdir weather_dir

$ javac -classpath ${HADOOP_HOME}/hadoop-${HADOOP_VERSION}-core.jar -d weather_classes Weather.java

$ jar -cvf /home/hadoop/weather.jar -C weather_classes/