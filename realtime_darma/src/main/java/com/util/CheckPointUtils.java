package com.util;

import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @Package com.rb.utils.CheckPointUtils
 * @Author runbo.zhang
 * @Date 2025/4/28 17:57
 * @description:
 */
public class CheckPointUtils {
    public static void setCk(StreamExecutionEnvironment env){
        env.setParallelism(1);
        // 设置 3s 的 checkpoint 间隔
        System.setProperty("HADOOP_USER_NAME", "hdfs");
        env.enableCheckpointing(3000);
        env.setStateBackend(new HashMapStateBackend());
        env.getCheckpointConfig().setCheckpointStorage("hdfs://cdh01:8020/flink/checkpoints/ck");
    }
    public static void newSetCk(StreamExecutionEnvironment env ,String s){
        env.setParallelism(1);
        // 设置 3s 的 checkpoint 间隔
        System.setProperty("HADOOP_USER_NAME", "hdfs");
        env.enableCheckpointing(3000);
        env.setStateBackend(new HashMapStateBackend());
        env.getCheckpointConfig().setCheckpointStorage("hdfs://cdh01:8020/flink/checkpoints/ck/"+s);
        env.getCheckpointConfig().setCheckpointTimeout(60000);

// 允许两个连续的 checkpoint 错误
        env.getCheckpointConfig().setTolerableCheckpointFailureNumber(2);

// 同一时间只允许一个 checkpoint 进行
        env.getCheckpointConfig().setMaxConcurrentCheckpoints(1);

    }
}