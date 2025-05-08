package com.zpy.stream.realtime.v1.app.dws;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.v1.bean.CartAddUuBean;
import com.v1.function.BeanToJsonStrMapFunction;
import com.v1.utils.DateFormatUtil;
import com.v1.utils.FlinkSinkUtil;
import com.v1.utils.FlinkSourceUtil;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.AllWindowedStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.AllWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

/**
 * 购物车独立用户统计窗口程序
 * 功能：统计每日独立用户购物车添加行为
 * 主要任务:
 * 1.计算每天有多少不同的用户执行了"添加购物车"操作
 * 2.确保同一用户在同一天内多次加购只计为1次（去重统计）
 * 3.结果按时间窗口聚合后输出
 */
public class DwsTradeCartAddUuWindow {
    public static void main(String[] args) throws Exception {
        // 1. 创建Flink流式执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 设置并行度为1，确保全局统计准确
        env.setParallelism(1);

        // 启用检查点机制，5秒一次，精确一次语义
        env.enableCheckpointing(5000L, CheckpointingMode.EXACTLY_ONCE);

        // 设置重启策略，失败后最多重启3次，每次间隔3秒
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3, 3000L));

        // 2. 数据源配置
        // 从Kafka获取购物车添加数据，消费者组ID为"dws_trade_cart_add_uu_window"
        KafkaSource<String> kafkaSource = FlinkSourceUtil.getKafkaSource(
                "dwd_trade_cart_add_pengyu_zhu",
                "dws_trade_cart_add_uu_window"
        );

        // 创建Kafka数据流
        DataStreamSource<String> kafkaStrDS = env.fromSource(
                kafkaSource,
                WatermarkStrategy.noWatermarks(),
                "Kafka_Source"
        );

        // 3. 数据转换处理
        // 将JSON字符串转换为JSONObject
        SingleOutputStreamOperator<JSONObject> jsonObjDS = kafkaStrDS.map(JSON::parseObject);

        // 分配时间戳和水位线
        SingleOutputStreamOperator<JSONObject> withWatermarkDS = jsonObjDS.assignTimestampsAndWatermarks(
                WatermarkStrategy
                        .<JSONObject>forMonotonousTimestamps()  // 单调递增的水位线策略
                        .withTimestampAssigner(  // 从数据中提取事件时间
                                new SerializableTimestampAssigner<JSONObject>() {
                                    @Override
                                    public long extractTimestamp(JSONObject jsonObj, long recordTimestamp) {
                                        // 使用ts_ms字段作为事件时间，转换为毫秒
                                        return jsonObj.getLong("ts_ms") * 1000;
                                    }
                                }
                        )
        );

        // 4. 按用户ID分组
        KeyedStream<JSONObject, String> keyedDS = withWatermarkDS.keyBy(
                jsonObj -> jsonObj.getString("user_id")
        );

        // 5. 使用状态去重，统计每日独立用户
        SingleOutputStreamOperator<JSONObject> cartUUDS = keyedDS.process(
                new KeyedProcessFunction<String, JSONObject, JSONObject>() {
                    // 定义状态变量，存储用户上次加购日期
                    private ValueState<String> lastCartDateState;

                    @Override
                    public void open(Configuration parameters) {
                        // 初始化状态描述符
                        ValueStateDescriptor<String> valueStateDescriptor =
                                new ValueStateDescriptor<>("lastCartDateState", String.class);
                        // 设置状态TTL为1天
                        valueStateDescriptor.enableTimeToLive(
                                StateTtlConfig.newBuilder(Time.days(1)).build()
                        );
                        // 获取状态
                        lastCartDateState = getRuntimeContext().getState(valueStateDescriptor);
                    }

                    @Override
                    public void processElement(
                            JSONObject jsonObj,
                            Context ctx,
                            Collector<JSONObject> out
                    ) throws Exception {
                        // 从状态中获取用户上次加购日期
                        String lastCartDate = lastCartDateState.value();
                        // 获取当前记录的加购日期
                        Long ts = jsonObj.getLong("ts_ms");
                        String curCartDate = DateFormatUtil.tsToDate(ts);

                        // 如果用户当天没有加购记录，则输出并更新状态
                        if (StringUtils.isEmpty(lastCartDate) || !lastCartDate.equals(curCartDate)) {
                            out.collect(jsonObj);
                            lastCartDateState.update(curCartDate);
                        }
                    }
                }
        );

        // 6. 窗口统计
        // 定义2秒的滚动事件时间窗口
        AllWindowedStream<JSONObject, TimeWindow> windowDS = cartUUDS
                .windowAll(TumblingEventTimeWindows.of(
                        org.apache.flink.streaming.api.windowing.time.Time.seconds(2)
                ));

        // 窗口聚合计算
        SingleOutputStreamOperator<CartAddUuBean> aggregateDS = windowDS.aggregate(
                // 聚合函数：统计窗口内记录数
                new AggregateFunction<JSONObject, Long, Long>() {
                    @Override
                    public Long createAccumulator() {
                        return 0L;  // 初始化为0
                    }

                    @Override
                    public Long add(JSONObject value, Long accumulator) {
                        return ++accumulator;  // 每条记录加1
                    }

                    @Override
                    public Long getResult(Long accumulator) {
                        return accumulator;  // 返回最终结果
                    }

                    @Override
                    public Long merge(Long a, Long b) {
                        return null;  // 会话窗口使用，这里不需要
                    }
                },
                // 窗口函数：包装结果
                new AllWindowFunction<Long, CartAddUuBean, TimeWindow>() {
                    @Override
                    public void apply(
                            TimeWindow window,
                            Iterable<Long> values,
                            Collector<CartAddUuBean> out
                    ) {
                        // 获取窗口内的独立用户数
                        Long cartUUCt = values.iterator().next();

                        // 计算窗口时间信息
                        long startTs = window.getStart() / 1000;  // 窗口开始时间(秒)
                        long endTs = window.getEnd() / 1000;      // 窗口结束时间(秒)
                        String stt = DateFormatUtil.tsToDateTime(startTs);  // 格式化开始时间
                        String edt = DateFormatUtil.tsToDateTime(endTs);    // 格式化结束时间
                        String curDate = DateFormatUtil.tsToDate(startTs);  // 当前日期

                        // 输出统计结果
                        out.collect(new CartAddUuBean(
                                stt,
                                edt,
                                curDate,
                                cartUUCt
                        ));
                    }
                }
        );

        // 7. 结果输出
        // 将结果对象转换为JSON字符串
        SingleOutputStreamOperator<String> operator = aggregateDS
                .map(new BeanToJsonStrMapFunction<>());

        // 打印结果到控制台
        operator.print();

        // 将结果写入Doris数据库
        operator.sinkTo(FlinkSinkUtil.getDorisSink("dws_trade_cart_add_uu_window"));

        // 8. 执行任务
        env.execute("DwsTradeCartAddUuWindow");
    }
}