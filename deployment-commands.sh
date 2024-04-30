
# vm1: HDFS namenode, YARN resourcemanager, zookeeper
# vm2-16: HDFS datanode, YARN nodemanager
# vm17: reserved for running merger and collection metrics
# vm18 - 20: Kafka cluster


# on VM1 do:
sudo -s

hdfs --daemon stop namenode
yarn --daemon stop resourcemanager

hdfs --daemon start namenode
yarn --daemon start resourcemanager
cd /opt/zookeeper/bin
zkServer.sh start


# on VM2-16 do:
sudo -s
hdfs --daemon stop datanode
yarn --daemon stop nodemanager

hdfs --daemon start datanode
yarn --daemon start nodemanager

# on VM18-20 do:
cd /opt/kafka/bin
./kafka-server-start.sh -daemon ../config/server.properties

#now on VM1
yarn node -list  # you should be able to see 15 worker nodes

# create a flink session,  the resource config can be tweaked
# command for stopping sessions and checking status, refer to https://nightlies.apache.org/flink/flink-docs-master/docs/deployment/resource-providers/yarn/
/opt/flink-1.19.0/bin/yarn-session.sh -Dtaskmanager.memory.process.size="3000 mb" -Dtaskmanager.memory.flink.size="2000 mb" --detached
# optionally you can see a log line indicating where the UI app is hosted, you can port-forward to your localhost to access UI



#now scp your jar file to vm1 and vm17
# For building jars with IntelliJ, refer to https://www.jetbrains.com/help/idea/compiling-applications.html#run_packaged_jar

#on vm17, start merger, this will create the two output topics (two needs to be different from existing topics)
java -jar <jar_file_name> runMerger kafka_server=sp24-cs525-2118.cs.illinois.edu:9092 flink_topic=<flink_output_topic> merger_topic=<merger_output_topic>


#on vm1, start conflux
/opt/flink-1.19.0/bin/flink run <jar_file_name> runConflux kafka_server=sp24-cs525-2118.cs.illinois.edu:9092 flink_topic=<flink_output_topic>
# you should now start to see the pipeline in UI


# once pipeline and merger log indicates a completion, collect metric on vm17
java -jar <jar_file_name> runMetric kafka_server=sp24-cs525-2118.cs.illinois.edu:9092  metric_topic=<merger_output_topic>
# a csv will be generated locally



# for running baseline, on vm1 do
/opt/flink-1.19.0/bin/flink run <jar_file_name> runBaseline kafka_server=sp24-cs525-2118.cs.illinois.edu:9092 flink_topic=<flink_output_topic>
#on vm17 do
java -jar <jar_file_name> runMetric kafka_server=sp24-cs525-2118.cs.illinois.edu:9092  metric_topic=<flink_output_topic>



#other caveats: some times kafka server will run out of space on /tmp. When the directory is about full, remove log dir and restart
cd /opt/kafka/bin
./kafka-server-stop.sh -daemon ../config/server.properties
rm -rf /tmp/kafka-logs
./kafka-server-start.sh -daemon ../config/server.properties






