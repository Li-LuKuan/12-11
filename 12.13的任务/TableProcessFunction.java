package com.atguigu.gamll.realtime.dim.function;

import com.alibaba.fastjson.JSONObject;
import com.atguigu.gamll.realtime.common.bean.TableProcessDim;
import com.atguigu.gamll.realtime.common.constant.Constant;
import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReadOnlyBroadcastState;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.hadoop.yarn.webapp.hamlet2.Hamlet;

import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.util.*;

public class TableProcessFunction extends BroadcastProcessFunction<JSONObject, TableProcessDim, Tuple2<JSONObject,TableProcessDim>> {

    private MapStateDescriptor<String, TableProcessDim> mapStateDescriptor;
    private Map<String,TableProcessDim> configMap = new HashMap<>();

    public TableProcessFunction(MapStateDescriptor<String, TableProcessDim> mapStateDescriptor){
        this.mapStateDescriptor = mapStateDescriptor;
    }
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

