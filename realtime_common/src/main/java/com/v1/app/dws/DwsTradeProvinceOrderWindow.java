package com.v1.app.dws;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.v1.bean.TradeProvinceOrderBean;
import com.v1.constant.Constant;
import com.v1.function.BeanToJsonStrMapFunction;
import com.v1.utils.DateFormatUtil;
import com.v1.utils.FlinkSinkUtil;
import com.v1.utils.FlinkSourceUtil;
import com.v1.utils.HBaseUtil;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.apache.hadoop.hbase.client.Connection;

import java.math.BigDecimal;
import java.util.Collections;
import java.util.HashSet;

/**
 * 数据来源：从Kafka消费订单明细数据(dwd_trade_order_detail)
 * 省份订单统计窗口程序
 * 功能：按省份统计订单金额和订单数量
 */
public class DwsTradeProvinceOrderWindow {
    public static void main(String[] args) throws Exception {
        // 1. 创建Flink执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 设置并行度为1，确保全局统计准确
        env.setParallelism(1);

        // 启用检查点机制，5秒一次，精确一次语义
        env.enableCheckpointing(5000L, CheckpointingMode.EXACTLY_ONCE);

        // 设置重启策略，失败后最多重启3次，每次间隔3秒
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3, 3000L));

        // 2. 数据源配置
        // 从Kafka获取订单明细数据，消费者组ID为"dws_trade_province_order_window"
        KafkaSource<String> kafkaSource = FlinkSourceUtil.getKafkaSource(
                "dwd_trade_order_detail_zhengwei_zhou",
                "dws_trade_province_order_window"
        );

        // 创建Kafka数据流
        DataStreamSource<String> kafkaStrDS = env.fromSource(
                kafkaSource,
                WatermarkStrategy.noWatermarks(),
                "Kafka_Source"
        );

        // 3. 数据转换处理
        // 将JSON字符串转换为JSONObject，并过滤空值
        SingleOutputStreamOperator<JSONObject> jsonObjDS = kafkaStrDS.process(
                new ProcessFunction<String, JSONObject>() {
                    @Override
                    public void processElement(String jsonStr, Context ctx, Collector<JSONObject> out) {
                        if (jsonStr != null) {
                            JSONObject jsonObj = JSON.parseObject(jsonStr);
                            out.collect(jsonObj);
                        }
                    }
                }
        );

        // 4. 数据去重处理
        // 按订单明细ID分组，处理重复数据
        KeyedStream<JSONObject, String> orderDetailIdKeyedDS = jsonObjDS.keyBy(jsonObj -> jsonObj.getString("id"));

        // 使用状态管理实现数据去重
        SingleOutputStreamOperator<JSONObject> distinctDS = orderDetailIdKeyedDS.process(
                new KeyedProcessFunction<String, JSONObject, JSONObject>() {
                    // 定义状态变量，存储最后处理的JSON对象
                    private ValueState<JSONObject> lastJsonObjState;

                    @Override
                    public void open(Configuration parameters) {
                        // 初始化状态描述符
                        ValueStateDescriptor<JSONObject> valueStateDescriptor =
                                new ValueStateDescriptor<>("lastJsonObjState", JSONObject.class);
                        // 设置状态TTL为10秒
                        valueStateDescriptor.enableTimeToLive(
                                StateTtlConfig.newBuilder(Time.seconds(10)).build()
                        );
                        // 获取状态
                        lastJsonObjState = getRuntimeContext().getState(valueStateDescriptor);
                    }

                    @Override
                    public void processElement(
                            JSONObject jsonObj,
                            Context ctx,
                            Collector<JSONObject> out
                    ) throws Exception {
                        // 获取状态中存储的上次处理的数据
                        JSONObject lastJsonObj = lastJsonObjState.value();

                        // 如果存在上次处理的数据，发送负金额用于抵消
                        if (lastJsonObj != null) {
                            String splitTotalAmount = lastJsonObj.getString("split_total_amount");
                            lastJsonObj.put("split_total_amount", "-" + splitTotalAmount);
                            out.collect(lastJsonObj);
                        }

                        // 更新状态并发送当前数据
                        lastJsonObjState.update(jsonObj);
                        out.collect(jsonObj);
                    }
                }
        );

        // 5. 分配时间戳和水位线
        SingleOutputStreamOperator<JSONObject> withWatermarkDS = distinctDS.assignTimestampsAndWatermarks(
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

        // 6. 转换为业务Bean对象
        SingleOutputStreamOperator<TradeProvinceOrderBean> beanDS = withWatermarkDS.map(
                new MapFunction<JSONObject, TradeProvinceOrderBean>() {
                    @Override
                    public TradeProvinceOrderBean map(JSONObject jsonObj) {
                        // 提取省份ID
                        String provinceId = jsonObj.getString("province_id");
                        // 提取订单金额
                        BigDecimal splitTotalAmount = jsonObj.getBigDecimal("split_total_amount");
                        // 提取时间戳
                        Long ts = jsonObj.getLong("ts_ms");
                        // 提取订单ID
                        String orderId = jsonObj.getString("order_id");

                        // 构建省份订单统计对象
                        return TradeProvinceOrderBean.builder()
                                .provinceId(provinceId)
                                .orderAmount(splitTotalAmount)
                                .orderIdSet(new HashSet<>(Collections.singleton(orderId)))
                                .build();
                    }
                }
        );

        // 7. 按省份ID分组
        KeyedStream<TradeProvinceOrderBean, String> provinceIdKeyedDS =
                beanDS.keyBy(TradeProvinceOrderBean::getProvinceId);

        // 8. 定义10秒的滚动事件时间窗口
        WindowedStream<TradeProvinceOrderBean, String, TimeWindow> windowDS = provinceIdKeyedDS
                .window(TumblingEventTimeWindows.of(
                        org.apache.flink.streaming.api.windowing.time.Time.seconds(10))
                );

        // 9. 窗口聚合计算
        SingleOutputStreamOperator<TradeProvinceOrderBean> reduceDS = windowDS.reduce(
                // 聚合函数：累加金额和订单ID集合
                new ReduceFunction<TradeProvinceOrderBean>() {
                    @Override
                    public TradeProvinceOrderBean reduce(TradeProvinceOrderBean value1, TradeProvinceOrderBean value2) {
                        // 累加订单金额
                        value1.setOrderAmount(value1.getOrderAmount().add(value2.getOrderAmount()));
                        // 合并订单ID集合
                        value1.getOrderIdSet().addAll(value2.getOrderIdSet());
                        return value1;
                    }
                },
                // 窗口函数：添加窗口时间信息
                new WindowFunction<TradeProvinceOrderBean, TradeProvinceOrderBean, String, TimeWindow>() {
                    @Override
                    public void apply(
                            String provinceId,
                            TimeWindow window,
                            Iterable<TradeProvinceOrderBean> input,
                            Collector<TradeProvinceOrderBean> out
                    ) {
                        // 获取聚合结果
                        TradeProvinceOrderBean orderBean = input.iterator().next();

                        // 计算窗口时间信息
                        long startTs = window.getStart() / 1000;  // 窗口开始时间(秒)
                        long endTs = window.getEnd() / 1000;      // 窗口结束时间(秒)
                        String stt = DateFormatUtil.tsToDateTime(startTs);  // 格式化开始时间
                        String edt = DateFormatUtil.tsToDateTime(endTs);    // 格式化结束时间
                        String curDate = DateFormatUtil.tsToDate(startTs);  // 当前日期

                        // 设置结果对象属性
                        orderBean.setStt(stt);
                        orderBean.setEdt(edt);
                        orderBean.setCurDate(curDate);
                        // 计算订单数量(去重后的订单ID集合大小)
                        orderBean.setOrderCount((long) orderBean.getOrderIdSet().size());

                        // 输出结果
                        out.collect(orderBean);
                    }
                }
        );

        // 10. 关联省份维度信息
        SingleOutputStreamOperator<TradeProvinceOrderBean> withProvinceDS = reduceDS.map(
                // 使用RichMapFunction获取HBase连接
                new RichMapFunction<TradeProvinceOrderBean, TradeProvinceOrderBean>() {
                    private Connection hbaseConn;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        // 初始化HBase连接
                        hbaseConn = HBaseUtil.getHBaseConnection();
                    }

                    @Override
                    public void close() throws Exception {
                        // 关闭HBase连接
                        HBaseUtil.closeHBaseConnection(hbaseConn);
                    }

                    @Override
                    public TradeProvinceOrderBean map(TradeProvinceOrderBean orderBean) throws Exception {
                        // 从HBase获取省份详细信息
                        String provinceId = orderBean.getProvinceId();
                        JSONObject provinceInfo = HBaseUtil.getRow(
                                hbaseConn,
                                Constant.HBASE_NAMESPACE,
                                "dim_base_province",
                                provinceId,
                                JSONObject.class
                        );
                        // 设置省份名称
                        orderBean.setProvinceName(provinceInfo.getString("name"));
                        return orderBean;
                    }
                }
        ).setParallelism(1);  // 限制并行度为1，避免过多HBase连接

        // 11. 结果输出
        // 将结果对象转换为JSON字符串
        SingleOutputStreamOperator<String> sink = withProvinceDS.map(
                new BeanToJsonStrMapFunction<>()
        );

        // 打印结果到控制台(调试用)
        sink.print();

        // 将结果写入Doris数据库
        sink.sinkTo(FlinkSinkUtil.getDorisSink("dws_trade_province_order_window"));

        // 12. 执行任务
        env.execute("DwsTradeProvinceOrderWindow");
    }
}