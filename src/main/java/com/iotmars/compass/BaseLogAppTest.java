package com.iotmars.compass;

import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;


/**
 * @author CJ
 * @date: 2022/9/28 10:43
 */
public class BaseLogAppTest {

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


        tableEnv.executeSql("CREATE TEMPORARY VIEW KafkaTable AS SELECT from_unixtime(1666270071, 'yyyy-MM-dd HH:mm') as df");


        tableEnv.executeSql("CREATE TABLE PrintTable (\n" +
                "    df String" +
                ") WITH (\n" +
                "    'connector' = 'print'\n" +
                ")"
        );

//        Table table = tableEnv.sqlQuery("select `deviceType`,`iotId`,`gmtCreate` from KafkaTable");
//        Table table = tableEnv.from("KafkaTable");
//        table.executeInsert("PrintTable");
        tableEnv.executeSql("insert into PrintTable select * from KafkaTable");

//        env.execute("kafkatest");
    }

}
