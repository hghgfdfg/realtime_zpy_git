package com.zpy.stream.realtime.v1.app.dws;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.v1.bean.TradeSkuOrderBean;
import com.v1.constant.Constant;
import com.v1.function.BeanToJsonStrMapFunction;
import com.v1.utils.DateFormatUtil;
import com.v1.utils.FlinkSinkUtil;
import com.v1.utils.FlinkSourceUtil;
import com.v1.utils.HBaseUtil;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
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
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.apache.hadoop.hbase.client.Connection;

/**
 * DWS层：商品粒度订单统计窗口计算
 * 功能：从Kafka消费订单明细数据，按SKU维度进行10秒滚动窗口聚合，补充商品维度信息后写入Doris
 */
public class DwsTradeSkuOrderWindow {
    public static void main(String[] args) throws Exception {
        // 1. 初始化Flink环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);  // 设置并行度为1（生产环境应根据资源调整）
        env.enableCheckpointing(5000L,  CheckpointingMode.EXACTLY_ONCE); // 开启5秒一次的精确一次检查点
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3,  3000L)); // 失败重启策略

        // 2. 从Kafka读取订单明细数据
        KafkaSource<String> kafkaSource = FlinkSourceUtil.getKafkaSource(
                "dwd_trade_order_detail_zhengwei_zhou", // 源Topic
                "dws_trade_sku_order_window" // 消费者组
        );
        DataStreamSource<String> kafkaStrDS = env.fromSource(
                kafkaSource,
                WatermarkStrategy.noWatermarks(),
                "Kafka_Source"
        );

        // 3. 数据预处理：JSON字符串转JSONObject
        SingleOutputStreamOperator<JSONObject> jsonObjDS = kafkaStrDS.process(
                new ProcessFunction<String, JSONObject>() {
                    @Override
                    public void processElement(String value, Context ctx, Collector<JSONObject> out) {
                        if (value != null) {
                            JSONObject jsonObj = JSON.parseObject(value);
                            out.collect(jsonObj);
                        }
                    }
                }
        );

        // 4. 数据去重：基于订单明细ID去重（保留最新版本）
        KeyedStream<JSONObject, String> orderDetailIdKeyedDS = jsonObjDS.keyBy(jsonObj  -> jsonObj.getString("id"));
        SingleOutputStreamOperator<JSONObject> distinctDS = orderDetailIdKeyedDS.process(
                new KeyedProcessFunction<String, JSONObject, JSONObject>() {
                    private ValueState<JSONObject> lastJsonObjState;

                    @Override
                    public void open(Configuration parameters) {
                        // 初始化状态：存储每个订单明细的最新版本
                        ValueStateDescriptor<JSONObject> valueStateDescriptor =
                                new ValueStateDescriptor<>("lastJsonObjState", JSONObject.class);
                        lastJsonObjState = getRuntimeContext().getState(valueStateDescriptor);
                    }

                    @Override
                    public void processElement(JSONObject jsonObj, Context ctx, Collector<JSONObject> out) throws Exception {
                        JSONObject lastJsonObj = lastJsonObjState.value();
                        if (lastJsonObj == null) {
                            // 首次收到该订单明细，存入状态并注册5秒后触发的定时器
                            lastJsonObjState.update(jsonObj);
                            long currentProcessingTime = ctx.timerService().currentProcessingTime();
                            ctx.timerService().registerProcessingTimeTimer(currentProcessingTime  + 5000L);
                        } else {
                            // 比较时间戳，保留更新的数据
                            String lastTs = lastJsonObj.getString("ts_ms");
                            String curTs = jsonObj.getString("ts_ms");
                            if (curTs.compareTo(lastTs)  >= 0) {
                                lastJsonObjState.update(jsonObj);
                            }
                        }
                    }

                    @Override
                    public void onTimer(long timestamp, OnTimerContext ctx, Collector<JSONObject> out) throws Exception {
                        // 定时器触发时输出最新数据并清空状态
                        JSONObject jsonObj = lastJsonObjState.value();
                        if (jsonObj != null) {
                            out.collect(jsonObj);
                            lastJsonObjState.clear();
                        }
                    }
                }
        );

        // 5. 分配事件时间和水印（单调递增水印策略）
        SingleOutputStreamOperator<JSONObject> withWatermarkDS = distinctDS.assignTimestampsAndWatermarks(
                WatermarkStrategy.<JSONObject>forMonotonousTimestamps()
                        .withTimestampAssigner(
                                (jsonObj, recordTimestamp) -> jsonObj.getLong("ts_ms")  * 1000 // 提取事件时间（毫秒）
                        )
        );

        // 6. 数据转换：JSONObject -> TradeSkuOrderBean
        SingleOutputStreamOperator<TradeSkuOrderBean> beanDS = withWatermarkDS.map(
                new MapFunction<JSONObject, TradeSkuOrderBean>() {
                    @Override
                    public TradeSkuOrderBean map(JSONObject jsonObj) {
                        return TradeSkuOrderBean.builder()
                                .skuId(jsonObj.getString("sku_id"))
                                .originalAmount(jsonObj.getBigDecimal("split_original_amount"))
                                .couponReduceAmount(jsonObj.getBigDecimal("split_coupon_amount"))
                                .activityReduceAmount(jsonObj.getBigDecimal("split_activity_amount"))
                                .orderAmount(jsonObj.getBigDecimal("split_total_amount"))
                                .ts_ms(jsonObj.getLong("ts_ms")  * 1000)
                                .build();
                    }
                }
        );

        // 7. 按SKU ID分组 + 10秒滚动窗口
        KeyedStream<TradeSkuOrderBean, String> skuIdKeyedDS = beanDS.keyBy(TradeSkuOrderBean::getSkuId);
        WindowedStream<TradeSkuOrderBean, String, TimeWindow> windowDS = skuIdKeyedDS
                .window(TumblingProcessingTimeWindows.of(org.apache.flink.streaming.api.windowing.time.Time.seconds(10)));

        // 8. 窗口聚合：计算每个SKU的订单金额指标
        SingleOutputStreamOperator<TradeSkuOrderBean> reduceDS = windowDS.reduce(
                // 增量聚合函数
                new ReduceFunction<TradeSkuOrderBean>() {
                    @Override
                    public TradeSkuOrderBean reduce(TradeSkuOrderBean value1, TradeSkuOrderBean value2) {
                        value1.setOriginalAmount(value1.getOriginalAmount().add(value2.getOriginalAmount()));
                        value1.setActivityReduceAmount(value1.getActivityReduceAmount().add(value2.getActivityReduceAmount()));
                        value1.setCouponReduceAmount(value1.getCouponReduceAmount().add(value2.getCouponReduceAmount()));
                        value1.setOrderAmount(value1.getOrderAmount().add(value2.getOrderAmount()));
                        return value1;
                    }
                },
                // 全窗口函数（补充窗口时间信息）
                new ProcessWindowFunction<TradeSkuOrderBean, TradeSkuOrderBean, String, TimeWindow>() {
                    @Override
                    public void process(String s, Context context, Iterable<TradeSkuOrderBean> elements, Collector<TradeSkuOrderBean> out) {
                        TradeSkuOrderBean orderBean = elements.iterator().next();
                        TimeWindow window = context.window();
                        orderBean.setStt(DateFormatUtil.tsToDateTime(window.getStart()));  // 窗口开始时间
                        orderBean.setEdt(DateFormatUtil.tsToDateTime(window.getEnd()));    // 窗口结束时间
                        orderBean.setCurDate(DateFormatUtil.tsToDate(window.getStart()));  // 窗口日期
                        out.collect(orderBean);
                    }
                }
        );

        // 9. 维度关联：补充SPU信息（从HBase维表）
        SingleOutputStreamOperator<TradeSkuOrderBean> withSpuInfoDS = reduceDS.map(
                new RichMapFunction<TradeSkuOrderBean, TradeSkuOrderBean>() {
                    private transient Connection hbaseConn;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        hbaseConn = HBaseUtil.getHBaseConnection();  // 获取HBase连接
                    }

                    @Override
                    public TradeSkuOrderBean map(TradeSkuOrderBean orderBean) {
                        String spuId = orderBean.getSpuId();
                        if (spuId != null) {
                            JSONObject spuInfo = HBaseUtil.getRow(
                                    hbaseConn,
                                    Constant.HBASE_NAMESPACE,
                                    "dim_spu_info",
                                    spuId,
                                    JSONObject.class
                            );
                            orderBean.setSpuName(spuInfo.getString("spu_name"));
                        }
                        return orderBean;
                    }

                    @Override
                    public void close() throws Exception {
                        HBaseUtil.closeHBaseConnection(hbaseConn);  // 关闭连接
                    }
                }
        );

        // 10. 维度关联：补充品牌信息（从HBase维表）
        SingleOutputStreamOperator<TradeSkuOrderBean> withTmDS = withSpuInfoDS.map(
                new RichMapFunction<TradeSkuOrderBean, TradeSkuOrderBean>() {
                    private transient Connection hbaseConn;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        hbaseConn = HBaseUtil.getHBaseConnection();
                    }

                    @Override
                    public TradeSkuOrderBean map(TradeSkuOrderBean orderBean) throws Exception {
                        String tmId = orderBean.getTrademarkId();
                        if (tmId != null) {
                            JSONObject tmInfo = HBaseUtil.getRow(
                                    hbaseConn,
                                    Constant.HBASE_NAMESPACE,
                                    "dim_base_trademark",
                                    tmId,
                                    JSONObject.class
                            );
                            orderBean.setTrademarkName(tmInfo.getString("tm_name"));
                        }
                        return orderBean;
                    }

                    @Override
                    public void close() throws Exception {
                        HBaseUtil.closeHBaseConnection(hbaseConn);
                    }
                }
        );

        // 11. 维度关联：三级分类信息（级联查询二级和一级分类）
        SingleOutputStreamOperator<TradeSkuOrderBean> finalDS = withTmDS
                // 关联三级分类
                .map(new RichMapFunction<TradeSkuOrderBean, TradeSkuOrderBean>() {
                    private transient Connection hbaseConn;

                    @Override public void open(Configuration parameters) throws Exception {
                        hbaseConn = HBaseUtil.getHBaseConnection();
                    }

                    @Override public TradeSkuOrderBean map(TradeSkuOrderBean orderBean) throws Exception {
                        String category3Id = orderBean.getCategory3Id();
                        if (category3Id != null) {
                            JSONObject category3Info = HBaseUtil.getRow(
                                    hbaseConn,
                                    Constant.HBASE_NAMESPACE,
                                    "dim_base_category3",
                                    category3Id,
                                    JSONObject.class
                            );
                            orderBean.setCategory3Name(category3Info.getString("name"));
                            orderBean.setCategory2Id(category3Info.getString("category2_id"));  // 准备二级分类查询
                        }
                        return orderBean;
                    }

                    @Override public void close() throws Exception {
                        HBaseUtil.closeHBaseConnection(hbaseConn);
                    }
                })
                // 关联二级分类
                .map(new RichMapFunction<TradeSkuOrderBean, TradeSkuOrderBean>() {
                    private transient Connection hbaseConn;

                    @Override public void open(Configuration parameters) throws Exception {
                        hbaseConn = HBaseUtil.getHBaseConnection();
                    }

                    @Override public TradeSkuOrderBean map(TradeSkuOrderBean orderBean) throws Exception {
                        String category2Id = orderBean.getCategory2Id();
                        if (category2Id != null) {
                            JSONObject category2Info = HBaseUtil.getRow(
                                    hbaseConn,
                                    Constant.HBASE_NAMESPACE,
                                    "dim_base_category2",
                                    category2Id,
                                    JSONObject.class
                            );
                            orderBean.setCategory2Name(category2Info.getString("name"));
                            orderBean.setCategory1Id(category2Info.getString("category1_id"));  // 准备一级分类查询
                        }
                        return orderBean;
                    }

                    @Override public void close() throws Exception {
                        HBaseUtil.closeHBaseConnection(hbaseConn);
                    }
                })
                // 关联一级分类
                .map(new RichMapFunction<TradeSkuOrderBean, TradeSkuOrderBean>() {
                    private transient Connection hbaseConn;

                    @Override public void open(Configuration parameters) throws Exception {
                        hbaseConn = HBaseUtil.getHBaseConnection();
                    }

                    @Override public TradeSkuOrderBean map(TradeSkuOrderBean orderBean) throws Exception {
                        String category1Id = orderBean.getCategory1Id();
                        if (category1Id != null) {
                            JSONObject category1Info = HBaseUtil.getRow(
                                    hbaseConn,
                                    Constant.HBASE_NAMESPACE,
                                    "dim_base_category1",
                                    category1Id,
                                    JSONObject.class
                            );
                            orderBean.setCategory1Name(category1Info.getString("name"));
                        }
                        return orderBean;
                    }

                    @Override public void close() throws Exception {
                        HBaseUtil.closeHBaseConnection(hbaseConn);
                    }
                });

        // 12. 数据输出
        finalDS.print();  // 控制台打印（调试用）

        // 转换为JSON字符串并写入Doris
        SingleOutputStreamOperator<String> jsonStrDS = finalDS.map(new  BeanToJsonStrMapFunction<>());
        jsonStrDS.sinkTo(FlinkSinkUtil.getDorisSink("dws_trade_sku_order_window"));

        // 13. 启动作业
        env.execute("DwsTradeSkuOrderWindow");
    }
}