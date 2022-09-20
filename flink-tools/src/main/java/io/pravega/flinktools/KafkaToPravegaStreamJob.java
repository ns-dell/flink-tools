package io.pravega.flinktools;

import io.pravega.connectors.flink.FlinkPravegaWriter;
import io.pravega.connectors.flink.PravegaWriterMode;
import io.pravega.flinktools.util.ByteArraySerializationFormat;
import io.pravega.flinktools.util.Filters;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.connector.kafka.source.reader.deserializer.KafkaRecordDeserializationSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class KafkaToPravegaStreamJob extends AbstractJob{
    final private static Logger log = LoggerFactory.getLogger(KafkaToPravegaStreamJob.class);
    public static ParameterTool parameterTool;
    /**
     * The entry point for Flink applications.
     *
     * @param args Command line arguments
     */
    public static void main(String... args) throws Exception {
        AppConfiguration config = new AppConfiguration(args);
        log.info("config: {}", config);
        parameterTool = ParameterTool.fromArgs(args);
        KafkaToPravegaStreamJob job = new KafkaToPravegaStreamJob(config);
        job.run();
    }

    public KafkaToPravegaStreamJob(AppConfiguration appConfiguration) {
        super(appConfiguration);
    }

    public void run() {
        try {
            final String jobName = getConfig().getJobName(KafkaToPravegaStreamJob.class.getName());

            final AppConfiguration.StreamConfig outputStreamConfig = getConfig().getStreamConfig("output");
            log.info("output stream: {}", outputStreamConfig);
            createStream(outputStreamConfig);
            final String fixedRoutingKey = getConfig().getParams().get("fixedRoutingKey", "");
            log.info("fixedRoutingKey: {}", fixedRoutingKey);

            final StreamExecutionEnvironment env = initializeFlinkStreaming();

            final StreamExecutionEnvironment kafkaEnv = StreamExecutionEnvironment.getExecutionEnvironment();
            kafkaEnv.getConfig().setRestartStrategy(RestartStrategies.fixedDelayRestart(4,10000));
            env.enableCheckpointing(5000); // create a checkpoint every 5 seconds
            env.getConfig()
                    .setGlobalJobParameters(
                            parameterTool); // make parameters available in the web interface
            final DataStream<byte[]> events =
                    env.fromSource(
                            KafkaSource.<byte[]>builder()
                                    .setBootstrapServers(
                                            parameterTool
                                                    .getProperties()
                                                    .getProperty(
                                                            ProducerConfig.BOOTSTRAP_SERVERS_CONFIG))
                                    .setBounded(OffsetsInitializer.latest())
                                    .setDeserializer(
                                            KafkaRecordDeserializationSchema.valueOnly(
                                                    ByteArrayDeserializer.class))
                                    .setTopics(parameterTool.getRequired("input-topic"))
                                    .build(),
                            WatermarkStrategy.noWatermarks(),"kafka-source");

            final DataStream<byte[]> toOutput = Filters.dynamicByteArrayFilter(events, getConfig().getParams());

            final FlinkPravegaWriter<byte[]> sink = FlinkPravegaWriter.<byte[]>builder()
                    .withPravegaConfig(outputStreamConfig.getPravegaConfig())
                    .forStream(outputStreamConfig.getStream())
                    .withSerializationSchema(new ByteArraySerializationFormat())
                    .withEventRouter(event -> fixedRoutingKey)
                    .withWriterMode(PravegaWriterMode.EXACTLY_ONCE)
                    .build();
            toOutput
                    .addSink(sink)
                    .uid("pravega-writer")
                    .name("Pravega writer to " + outputStreamConfig.getStream().getScopedName());

            log.info("Executing {} job", jobName);
            env.execute(jobName);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
