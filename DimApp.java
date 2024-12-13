import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.gamll.realtime.common.bean.TableProcessDim;
import com.atguigu.gamll.realtime.common.constant.Constant;
import com.atguigu.gamll.realtime.common.util.JsonDebeziumDeserializationUtil;
import com.mysql.cj.xdevapi.DbDoc;
import com.mysql.cj.xdevapi.JsonString;
import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import com.ververica.cdc.connectors.mysql.table.StartupOptions;
import com.ververica.cdc.debezium.JsonDebeziumDeserializationSchema;
import com.ververica.cdc.debezium.StringDebeziumDeserializationSchema;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.util.Collector;
import org.apache.hadoop.fs.shell.Concat;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;

import java.io.IOException;
import java.util.Properties;

public class DimApp {
    public static void main(String[] args) throws Exception {
        //TODO 1.基本环境准备
        //指定流处理环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //设置并行度
        env.setParallelism(4);
        //TODO 2.检查点相关的设置
        //开启检查点
        env.enableCheckpointing(5000L, CheckpointingMode.EXACTLY_ONCE);
        //设置检查点超时时间
        env.getCheckpointConfig().setCheckpointTimeout(60000L);
        //设置job取消后检查点是否保留
        env.getCheckpointConfig().setExternalizedCheckpointCleanup(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
        //设置两个检查点之间最小时间间隔
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(2000L);
        //设置重启策略
//        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3,3000L));
        env.setRestartStrategy(RestartStrategies.failureRateRestart(3, Time.days(30),Time.seconds(3)));
        //设置状态后端以及检查点存储路径
        env.setStateBackend(new HashMapStateBackend());
        env.getCheckpointConfig().setCheckpointStorage("hdfs://hadoop102:8020/ck");
        //设置操作hadoop的用户
        System.setProperty("HADOOP_USER_NAME","root");

        //TODO 3.从kafaka的topic_db主题中读取业务数据
        //声明消费的主题以及消费者组
        String topic = "topic_log";
        String groupId = "dim_app_group";
        //创建消费者对象
        KafkaSource<String> kafkaSource = KafkaSource.<String>builder()
                .setBootstrapServers(Constant.KAFKA_BROKERS)
                .setTopics(Constant.TOPIC_DB)
                .setGroupId(groupId)
//                .setStartingOffsets(OffsetsInitializer.committedOffsets(OffsetResetStrategy.LATEST))
                // Start from latest offset
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setValueOnlyDeserializer(new DeserializationSchema<String>() {
                    @Override
                    public String deserialize(byte[] message) throws IOException {
//                        if(message !=null){
//                            return new String(message);
//                        }
                        return null;
                    }

                    @Override
                    public boolean isEndOfStream(String s) {
                        return false;
                    }

                    @Override
                    public TypeInformation<String> getProducedType() {
                        return TypeInformation.of(String.class);
                    }
                })
                .build();
        //消费数据 封装为流
        DataStreamSource<String> kafkaStrDS = env.fromSource(kafkaSource, WatermarkStrategy.noWatermarks(), "Kafka_Source");
        //对业务流中数据类型进行转换并进行简单的ETL  jsonStr->jsonObj
        SingleOutputStreamOperator<JSONObject> jsonObjDs = kafkaStrDS.process(
                new ProcessFunction<String, JSONObject>() {
                    @Override
                    public void processElement(String s, ProcessFunction<String, JSONObject>.Context context, Collector<JSONObject> out) throws Exception {
                        JSONObject jsonObject = JSON.parseObject(s);
                        String db = jsonObject.getString("database");
                        String type = jsonObject.getString("type");
                        String data = jsonObject.getString("data");
                        if ("gmall2023".equals(db)
                                && ("insert".equals(type)
                                || "update".equals(type)
                                || "delete".equals(type)
                                || "bootstrap-insert".equals(type))
                                && data != null
                                && data.length() > 2
                        ) {
                            out.collect(jsonObject);
                        }

                    }
                }
        );
//        kafkaStrDS.print();
//        jsonObjDs.print();

        Properties props = new Properties();
        props.setProperty("useSSL", "false");
        props.setProperty("allowPublicKeyRetrieval", "true");
        MySqlSource<String> mySqlSource = MySqlSource.<String>builder()
                .hostname(Constant.MYSQL_HOST)
                .port(Constant.MYSQL_PORT)
                .databaseList("gmall2023-config") // monitor all tables under inventory database
                .tableList("gmall2023-config.table_process_dim")
                .username(Constant.MYSQL_USER_NAME)
                .password(Constant.MYSQL_PASSWORD)
                .deserializer(new JsonDebeziumDeserializationSchema()) // converts SourceRecord to String
                .startupOptions(StartupOptions.initial())
                .jdbcProperties(props)
                .build();
        //读取数据 封装为流
        DataStreamSource<String> mysqlStrDs = env.fromSource(mySqlSource, WatermarkStrategy.noWatermarks(), "mysql_source");
//        mysqlStrDs.print();
        //对配置流中的数据类型进行转换 jsonStr->实体类对象
        SingleOutputStreamOperator<TableProcessDim> tpDs = mysqlStrDs.map(
                new MapFunction<String, TableProcessDim>() {
                    @Override
                    public TableProcessDim map(String s) throws Exception {
                        //为了处理方便，先将jsonStr也就是s转化为jsonObj
                        JSONObject jsonObj = JSON.parseObject(s);
                        String op = jsonObj.getString("op");
                        TableProcessDim tableProcessDim = null;
                        if ("d".equals(op)) {
                            //对配置表进行了一次删除操作，从before属性中获取删除前的配置
                            jsonObj.getObject("before", TableProcessDim.class);
                        } else {
                            //对配置表进行了读取、添加、修改操作   从after属性中获取最新的配置信息
                            tableProcessDim = jsonObj.getObject("after", TableProcessDim.class);
                        }
                        tableProcessDim.setOp(op);
                        return tableProcessDim;
                    }
                }
        ).setParallelism(1);
        //4> {"op":"r","data":{"sink_row_key":"id","sink_family":"info","sink_table":"dim_base_trademark","source_table":"base_trademark","sink_columns":"id,tm_name"},"db":"gmall2023-config","tb":"table_process_dim"}
        tpDs.print();


        env.execute();

    }
}
