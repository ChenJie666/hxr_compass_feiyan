package com.iotmars.compass;

import com.iotmars.compass.util.KafkaUtil;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.apache.flink.types.RowKind;

import java.awt.*;


/**
 * @author CJ
 * @date: 2022/9/28 10:43
 */
public class BaseLogApp {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        // 开启checkpoint，每隔3秒做一次ck，并制定ck的一致性语义
        env.enableCheckpointing(10000L, CheckpointingMode.EXACTLY_ONCE);
        // 设置ck超时时间为1min
        env.getCheckpointConfig().setCheckpointTimeout(60 * 1000L);
        // 设置两次重启的最小时间间隔
        env.getCheckpointConfig().setMaxConcurrentCheckpoints(1);
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(3000L);
        //
        env.getCheckpointConfig().enableExternalizedCheckpoints(
                CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION
        );

        env.setRestartStrategy(RestartStrategies.failureRateRestart(
                3, Time.days(1L), Time.minutes(1L)
        ));

        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

//        DataStreamSource<Row> sourceStream = env.fromElements(
//                Row.ofKind(RowKind.INSERT, "a", "A", "1"),
//                Row.ofKind(RowKind.INSERT, "b", "B", "2"),
//                Row.ofKind(RowKind.INSERT, "c", "C", "3"),
//                Row.ofKind(RowKind.UPDATE_BEFORE, "a", "A", "1"),
//                Row.ofKind(RowKind.UPDATE_AFTER, "a", "C", "3")
//        );
//
//        Table table1 = tableEnv.fromChangelogStream(sourceStream);
//        tableEnv.createTemporaryView("sourceTable",table1);
//        tableEnv.createTemporaryView("sourceTable",sourceStream,$("deviceType"),$("iotId"),$("gmtCreate"));


        tableEnv.executeSql("CREATE TABLE KafkaTable (\n" +
                        "   `deviceType` STRING,\n" +
                        "   `iotId` STRING,\n" +
                        "   `requestId` STRING,\n" +
                        "   `checkFailedData` STRING,\n" +
                        "   `productKey` STRING,\n" +
                        "   `gmtCreate` bigint,\n" +
                        "   `deviceName` STRING,\n" +
                        "   `items` STRING,\n" +
                        "   `ts` TIMESTAMP(3) METADATA FROM 'timestamp'\n" +
                        ") WITH (\n" +
                        "                'connector' = 'kafka',\n" +
                        "                'topic' = 'items-model',\n" +
                        "                'properties.bootstrap.servers' = '192.168.101.179:9092',\n" +
                        "                'properties.group.id' = 'testGroup',\n" +
//                "                'scan.startup.mode' = 'earliest-offset',\n" +
                        "                'scan.startup.mode' = 'latest-offset',\n" +
                        "                'format' = 'json'\n" +
                        "        )"
        );

        tableEnv.executeSql("CREATE VIEW cntTable AS SELECT count(*) as pv, count(distinct iotId) as uv, from_unixtime(cast(gmtCreate/1000 as int), 'yyyy-MM-dd HH:mm') as tm FROM KafkaTable GROUP BY from_unixtime(cast(gmtCreate/1000 as int), 'yyyy-MM-dd HH:mm')");


//        tableEnv.executeSql("CREATE TABLE PrintTable (\n" +
//                "   `deviceType` STRING,\n" +
//                "   `iotId` STRING,\n" +
//                "   `requestId` STRING,\n" +
//                "   `checkFailedData` STRING,\n" +
//                "   `productKey` STRING,\n" +
//                "   `gmtCreate` STRING,\n" +
//                "   `deviceName` STRING,\n" +
//                "   `items` STRING,\n" +
//                "   `ts` TIMESTAMP(3)\n" +
//                ") WITH (\n" +
//                "    'connector' = 'print'\n" +
//                ")"
//        );

                tableEnv.executeSql("CREATE TABLE PrintTable (\n" +
                "   `pv` bigint," +
                "   `uv` bigint," +
                "   `tm` string" +
                ") WITH (\n" +
                "    'connector' = 'print'\n" +
                ")"
        );

//        Table table = tableEnv.sqlQuery("select `deviceType`,`iotId`,`gmtCreate` from KafkaTable");
//        Table table = tableEnv.from("KafkaTable");
//        table.executeInsert("PrintTable");
        tableEnv.executeSql("insert into PrintTable select * from cntTable");

//        env.execute("kafkatest");
    }

}