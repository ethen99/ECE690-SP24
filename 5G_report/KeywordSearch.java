import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Properties;
import java.util.Set;

public class KeywordSearch {

    public static void main(String[] args) {
        // Kafka 配置
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "message-splitter");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());

        // 创建流构建器
        StreamsBuilder builder = new StreamsBuilder();

        // 从输入主题读取消息流
        KStream<String, String> inputStream = builder.stream("input-topic");

        // 过滤包含指定关键字的消息，发送到 NAS 主题
        Set<String> nasKeywords = new HashSet<>(Arrays.asList(
               "Authentication","Attach","Detach","Service","Identity"
                ));
        KStream<String, String> nasStream = filterAndSendToTopic(inputStream, nasKeywords, "nas-topic");

        // 过滤包含指定关键字的消息，发送到 RCC 主题
        Set<String> rrcKeywords = new HashSet<>(Arrays.asList(
                "RRCConnectionReconfiguration", "RRCConnectionReconfigurationComplete",
            "ULInformationTransfer", "RRCConnectionRequest", "RRCConnectionSetup", "RRCConnectionSetupComplete",
            "SecurityModeCommand",
            "SecurityModeComplete", "UECapabilityEnquiry", "UECapabilityInformation"
        ));
        KStream<String, String> rrcStream = filterAndSendToTopic(inputStream, rrcKeywords, "rcc-topic");

        // 构建 Kafka Streams 对象并启动
        KafkaStreams streams = new KafkaStreams(builder.build(), props);
        streams.start();

        // 程序运行时保持运行
        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
    }

    private static KStream<String, String> filterAndSendToTopic(KStream<String, String> inputStream, Set<String> keywords, String outputTopic) {
        KStream<String, String> filteredStream = inputStream.filter((key, value) -> containsKeyword(value, keywords));
        filteredStream.to(outputTopic);
        return filteredStream;
    }

    private static boolean containsKeyword(String value, Set<String> keywords) {
        for (String keyword : keywords) {
            if (value.toLowerCase().contains(keyword.toLowerCase())) {
                return true;
            }
        }
        return false;
    }
}

                            
