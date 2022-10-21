package com.iotmars.compass;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import static org.apache.flink.table.api.Expressions.$;

/**
 * @author CJ
 * @date: 2022/10/21 11:26
 */
public class TelnetTest {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        DataStreamSource<String> nc8 = env.socketTextStream("192.168.32.225", 9999);
        DataStreamSource<String> nc9 = env.socketTextStream("192.168.32.225", 9998);

        Table table8 = tableEnv.fromDataStream(nc8).select($("f0").as("s"));
        Table table9 = tableEnv.fromDataStream(nc9).select($("f0").as("s"));
        tableEnv.createTemporaryView("table8",table8);
        tableEnv.createTemporaryView("table9",table9);

        tableEnv.executeSql("CREATE VIEW userTable AS SELECT split_index(s,',',0) as id,split_index(s,',',1) as name FROM table8");

        tableEnv.executeSql("CREATE TABLE PrintTable(" +
                "id string," +
                "name string" +
                ") WITH (" +
                "'connector'='print'" +
                ")");

        tableEnv.executeSql("INSERT INTO PrintTable Select id,name FROM userTable");
    }
}
