package org.example.utils;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;

import java.util.Collections;
import java.util.Properties;

public class KafkaAdminUtils {

    public static void createTopic(String kafkaBootstrapServers, String topic, int pathNum){
        Properties prop = new Properties();
        prop.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaBootstrapServers);

        try (AdminClient adminClient = AdminClient.create(prop)) {
            NewTopic newTopic = new NewTopic(topic, pathNum, (short) 1);
            adminClient.createTopics(Collections.singleton(newTopic)).all().get();
            System.out.println("Topic created successfully");
        } catch (Exception e) {
            System.out.println("Topic probbably exists");
            e.printStackTrace();
        }
    }
}
