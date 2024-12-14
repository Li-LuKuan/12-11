package com.atguigu.gamll.realtime.dim.app;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.gamll.realtime.common.bean.TableProcessDim;
import com.atguigu.gamll.realtime.common.constant.Constant;
import com.atguigu.gamll.realtime.common.util.FlinkSourceUtil;
import com.atguigu.gamll.realtime.common.util.HBaseUtil;
import com.atguigu.gamll.realtime.common.util.JsonDebeziumDeserializationUtil;
import com.mysql.cj.xdevapi.DbDoc;
import com.mysql.cj.xdevapi.JsonString;
import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReadOnlyBroadcastState;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.BroadcastConnectedStream;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
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
import org.apache.hadoop.hbase.client.Connection;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.eclipse.jetty.webapp.MetaData;

import java.io.IOException;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.util.*;

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
//        String topic = "topic_log";
        String groupId = "dim_app_group";
        //创建消费者对象
        KafkaSource<String> kafkaSource = FlinkSourceUtil.getKafkaSource(Constant.TOPIC_DB, groupId);
        //消费数据 封装为流
        DataStreamSource<String> kafkaStrDS = env.fromSource(kafkaSource, WatermarkStrategy.noWatermarks(), "Kafka_Source");
        //TODO 4.对业务流中数据类型进行转换并进行简单的ETL  jsonStr->jsonObj
        SingleOutputStreamOperator<JSONObject> jsonObjDS = kafkaStrDS.process(
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
//        jsonObjDS.print();
        //TODO 5.使用FlinkCDC读取配置表中的配置信息
        //创建MySQLSource对象
        MySqlSource<String> mySqlSource = FlinkSourceUtil.getMySqlSource("gmall2023_config", "table_process_dim");
        //读取数据 封装为流
        DataStreamSource<String> mysqlStrDS = env.fromSource(mySqlSource, WatermarkStrategy.noWatermarks(), "mysql_source").setParallelism(1);
//        mysqlStrD.print();
        //对配置流中的数据类型进行转换 jsonStr->实体类对象
        SingleOutputStreamOperator<TableProcessDim> tpDS = mysqlStrDS.map(
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
//        tpDS.print();
        //根据配置表中的配置信息到HBase中执行建表或者删除表操作
        tpDS = tpDS.map(
                new RichMapFunction<TableProcessDim, TableProcessDim>() {

                    private Connection hbaseConn;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        hbaseConn = HBaseUtil.getHBaseConnection();
                    }

                    @Override
                    public void close() throws Exception {
                        HBaseUtil.closeHBaseConnetion(hbaseConn);
                    }

                    @Override
                    public TableProcessDim map(TableProcessDim tp) throws Exception {
                        //获取对配置表进行的操作的类型
                        String op = tp.getOp();
                        //获取Hbase中维度表的表名
                        String sinkTable = tp.getSinkTable();
                        //获取在HBase中建表的列族
                        String[] sinkFamilies = tp.getSinkFamily().split(",");
                        if ("d".equals(op)) {
                            //从配置表中删除了一条数据，将hbase中对应的表删除掉
                            HBaseUtil.dropHBaseTable(hbaseConn,Constant.HBASE_NAMESPACE,sinkTable);
                        } else if ("r".equals(op) || "c".equals(op)){
                            //从配置表中读取了一条数据或者向配置表中了一条配置      在hbase中执行建表
                            HBaseUtil.createHBaseTable(hbaseConn,Constant.HBASE_NAMESPACE,sinkTable,sinkFamilies);
                        } else {
                            //对配置表中的配置信息进行了修改     先从Hhbase中将对应的表删除掉，再创建新表
                            HBaseUtil.dropHBaseTable(hbaseConn,Constant.HBASE_NAMESPACE,sinkTable);
                            HBaseUtil.createHBaseTable(hbaseConn,Constant.HBASE_NAMESPACE,sinkTable,sinkFamilies);

                        }
                        return tp;
                    }
                }
        ).setParallelism(1);
//        tpDS.print();

        //TODO 8.将配置流中的配置信息进行广播---broadcast
        MapStateDescriptor<String, TableProcessDim> mapStateDescriptor
                = new MapStateDescriptor<String, TableProcessDim>("mapStateDescriptor",String.class,TableProcessDim.class);
        BroadcastStream<TableProcessDim> broadcastDS = tpDS.broadcast(mapStateDescriptor);

        //TODO 9.将主流业务数据和广播流配置信息进行关联---connect
        BroadcastConnectedStream<JSONObject, TableProcessDim> connectDS = jsonObjDS.connect(broadcastDS);
        //TODO 10.处理关联后的数据（判断是否为维度）
        SingleOutputStreamOperator<Tuple2<JSONObject,TableProcessDim>> dimDS = connectDS.process(
                new BroadcastProcessFunction<JSONObject, TableProcessDim, Tuple2<JSONObject,TableProcessDim>>() {

                    private Map<String,TableProcessDim> configMap = new HashMap<>();
                    @Override
                    public void open(Configuration parameters) throws Exception {
                        //将配置表中的配置信息预加载到程序configMap中
                        //注册驱动
                        Class.forName("com.mysql.cj.jdbc.Driver");
                        //建立连接
                        java.sql.Connection conn = DriverManager.getConnection(Constant.MYSQL_URL, Constant.MYSQL_USER_NAME, Constant.MYSQL_PASSWORD);
                        //获取数据库操作对象
                        String sql = "select * from gmall2023_config.table_process_dim";
                        PreparedStatement ps = conn.prepareStatement(sql);
                        //执行SQL语句
                        ResultSet rs = ps.executeQuery();
                        ResultSetMetaData metaData = rs.getMetaData();
                        //处理结果集
                        while (rs.next()){
                            //定义一个json对象， 用于接收遍历出来的数据
                            JSONObject jsonObj = new JSONObject();
                            for (int i = 1; i <= metaData.getColumnCount(); i++) {
                                String columnName = metaData.getColumnName(i);
                                Object columnValue = rs.getObject(i);
                                jsonObj.put(columnName,columnValue);
                            }
                            //将jsonObj转换为实体类对象，并放到configMap中
                            TableProcessDim tableProcessDim = jsonObj.toJavaObject(TableProcessDim.class);
                            configMap.put(tableProcessDim.getSourceTable(),tableProcessDim);
                        }
                        //释放资源
                        rs.close();
                        ps.close();
                        conn.close();
                    }

                    //处理主流业务数据      根据维度表名到广播状态中读取配置信息，判断是否为维度
                    @Override
                    public void processElement(JSONObject jsonObj, BroadcastProcessFunction<JSONObject, TableProcessDim, Tuple2<JSONObject,TableProcessDim>>.ReadOnlyContext ctx, Collector<Tuple2<JSONObject,TableProcessDim>> out) throws Exception {
                        //获取处理的数据的表名
                        String table = jsonObj.getString("table");
                        //获取广播状态
                        ReadOnlyBroadcastState<String, TableProcessDim> broadcastState = ctx.getBroadcastState(mapStateDescriptor);
                        //根据表名先到广播状态中获取对应的配置信息，如果没有找到对应的配置，在尝试到configMap中获取
                        TableProcessDim tableProcessDim = null;

                        if ((tableProcessDim = broadcastState.get(table)) != null
                                || (tableProcessDim = configMap.get(table))!= null){
                            //如果根据表名获取到了对应的配置信息，说明当前处理的是维度数据
                            // 将维度数据继续向下游传递（只需要传递data属性内容即可）
                            JSONObject dataJsonObj = jsonObj.getJSONObject("data");

                            //在向下游传递数据前，过滤掉不需要传递的属性
                            String sinkColumns = tableProcessDim.getSinkColumns();
                            deleteNotNeedColumns(dataJsonObj,sinkColumns);
                            //在向下游传递数据前，补充对维度数据的操作类型属性
                            String type = jsonObj.getString("type");
                            dataJsonObj.put("type",type);

                            out.collect(Tuple2.of(dataJsonObj,tableProcessDim));

                        }


                    }

                    //处理广播流配置信息     将配置数据放到广播状态中或者从广播状态中删除对应的配置   k:维度名    V:一个配置对象
                    @Override
                    public void processBroadcastElement(TableProcessDim tp, BroadcastProcessFunction<JSONObject, TableProcessDim, Tuple2<JSONObject,TableProcessDim>>.Context cxt, Collector<Tuple2<JSONObject,TableProcessDim>> out) throws Exception {
                        //获取对配置表进行的操作的类型
                        String op = tp.getOp();
                        //获取广播状态
                        BroadcastState<String, TableProcessDim> broadcastState = cxt.getBroadcastState(mapStateDescriptor);
                        //获取维度表名称
                        String sourceTable = tp.getSourceTable();
                        if ("d".equals(op)){
                            //从配置表中删除了一条数据，将对应的配置信息也从广播状态中删除
                            broadcastState.remove(sourceTable);
                            configMap.remove(sourceTable);
                        }else {
                            //对配置表进行了读取、添加、或者更新操作，将最新的配置信息放到广播状态中
                            broadcastState.put(sourceTable,tp);
                            configMap.put(sourceTable,tp);
                        }
                    }
                }
        );


        //TODO 11.将维度数据同步到HBase表中
        dimDS.print();


        dimDS.addSink(new RichSinkFunction<Tuple2<JSONObject, TableProcessDim>>() {
            private Connection hbaseConn;
            @Override
            public void open(Configuration parameters) throws Exception {
                hbaseConn = HBaseUtil.getHBaseConnection();
            }

            @Override
            public void close() throws Exception {
                HBaseUtil.closeHBaseConnetion(hbaseConn);
            }

            //将流中数据写出到HBase
            @Override
            public void invoke(Tuple2<JSONObject, TableProcessDim> tup, Context context) throws Exception {
                JSONObject jsonObj = tup.f0;
                TableProcessDim tableProcessDim = tup.f1;
                String type = jsonObj.getString("type");
                jsonObj.remove("type");

                //获取操作的Hbase表的表名
                String sinkTable = tableProcessDim.getSinkTable();
                //获取rowkey
                String rowKey = jsonObj.getString(tableProcessDim.getSinkRowKey());
                //判断对业务数据库维度表进行了什么操作
                if ("delete".equals(type)){
                    //从业务数据库维度表中做了删除    需要将HBase维度表中对应的记录也删除掉
                    HBaseUtil.delRow(hbaseConn,Constant.HBASE_NAMESPACE,sinkTable,rowKey);
                }else {
                    //如果不是delete， 可能的类型有insert、update、bootstrap-insert, 上述操作对应的都是向HBase表中put数据
                    String sinkFamily = tableProcessDim.getSinkFamily();
                    HBaseUtil.putRow(hbaseConn,Constant.HBASE_NAMESPACE,sinkTable,rowKey,sinkFamily,jsonObj);
                }
            }
        });


        env.execute();

    }

    //过滤掉不需要传递的字段
    private static void deleteNotNeedColumns(JSONObject dataJsonObj, String sinkColumns) {
        List<String> columnList = Arrays.asList(sinkColumns.split(","));
        Set<Map.Entry<String, Object>> entrySet = dataJsonObj.entrySet();

        entrySet.removeIf(entry-> !columnList.contains(entry.getKey()));
//        Iterator<Map.Entry<String, Object>> it = entrySet.iterator();
//        for (;it.hasNext();) {
//            Map.Entry<String, Object> entry = it.next();
//            if (!columnList.contains(entry.getKey())){
//                entrySet.remove(entry);
//            }
//        }
    }
}
