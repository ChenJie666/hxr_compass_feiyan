package com.iotmars.compass;

import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.time.ZoneId;


/**
 * @author CJ
 * @date: 2022/9/28 10:43
 */
public class BaseLogApp1 {

    public static void main(String[] args) throws Exception {
//        System.setProperty("user.timezone","GMT+8");

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
        tableEnv.getConfig().setLocalTimeZone(ZoneId.of("Asia/Shanghai"));

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


        tableEnv.executeSql("CREATE VIEW tsTable AS SELECT TO_TIMESTAMP_LTZ(1666250555,0) as ts,TO_TIMESTAMP_LTZ(1666250555,0) as ts1");
//        tableEnv.executeSql("CREATE VIEW tsTable AS SELECT current_timestamp as ts,current_timestamp as ts1,PROCTIME() as pro,PROCTIME() as pro1");

        tableEnv.executeSql("CREATE TABLE PrintTable (\n" +
                "   `ts` timestamp_ltz(9)," +
                "   `ts1` timestamp(9)" +
                ") WITH (\n" +
                "    'connector' = 'print'\n" +
                ")"
        );

//        Table table = tableEnv.sqlQuery("select `deviceType`,`iotId`,`gmtCreate` from KafkaTable");
//        Table table = tableEnv.from("KafkaTable");
//        table.executeInsert("PrintTable");
        tableEnv.executeSql("insert into PrintTable select * from tsTable");

//        env.execute("kafkatest");
    }

}