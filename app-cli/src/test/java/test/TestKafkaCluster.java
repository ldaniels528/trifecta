package test;

import kafka.server.KafkaConfig;
import kafka.server.KafkaServerStartable;
import org.apache.curator.test.TestingServer;

import java.io.IOException;
import java.util.Properties;

/**
 * Test Kafka Cluster
 * @author lawrence.daniels@gmail.com
 */
public class TestKafkaCluster {
    KafkaServerStartable kafkaServer;
    TestingServer zkServer;

    public TestKafkaCluster() throws Exception {
        zkServer = new TestingServer();
        KafkaConfig config = getKafkaConfig(zkServer.getConnectString());
        kafkaServer = new KafkaServerStartable(config);
        kafkaServer.startup();
    }

    private static KafkaConfig getKafkaConfig(final String zkConnectString) {
        final Properties props = new Properties();
        props.put("zookeeper.connect", zkConnectString);
        return new KafkaConfig(props);
    }

    public String getKafkaBrokerString() {
        return String.format("localhost:%d", kafkaServer.serverConfig().port());
    }

    public String getZkConnectString() {
        return zkServer.getConnectString();
    }

    public int getKafkaPort() {
        return kafkaServer.serverConfig().port();
    }

    public void stop() throws IOException {
        kafkaServer.shutdown();
        zkServer.stop();
    }
}