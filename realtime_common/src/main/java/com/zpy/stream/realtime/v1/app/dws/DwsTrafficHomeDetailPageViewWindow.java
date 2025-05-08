package com.zpy.stream.realtime.v1.app.dws;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.v1.bean.TrafficHomeDetailPageViewBean;
import com.v1.function.BeanToJsonStrMapFunction;
import com.v1.utils.DateFormatUtil;
import com.v1.utils.FlinkSinkUtil;
import com.v1.utils.FlinkSourceUtil;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
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
import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

/**
 * DWS层：首页与商品详情页独立访客统计
 * 功能：统计每10秒窗口内访问首页和商品详情页的独立用户数（UV）
 * 数据流：Kafka(dwd层页面日志) → Flink实时处理 → Doris(dws层聚合结果)
 */
public class DwsTrafficHomeDetailPageViewWindow {
    public static void main(String[] args) throws Exception {
        // === 1. 初始化Flink环境 ===
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);  // 生产环境建议根据集群资源调整
        env.enableCheckpointing(5000L,  CheckpointingMode.EXACTLY_ONCE); // 5秒一次精确一次检查点

        // === 2. 数据源配置 ===
        KafkaSource<String> kafkaSource = FlinkSourceUtil.getKafkaSource(
                "dwd_traffic_page_zhengwei_zhou", // 原始页面日志Topic
                "dws_traffic_home_detail_page_view_window" // 消费者组
        );
        DataStreamSource<String> kafkaStrDS = env.fromSource(
                kafkaSource,
                WatermarkStrategy.noWatermarks(),
                "Kafka_PageLog_Source"
        );

        // === 3. 数据预处理 ===
        // 3.1 JSON字符串转对象
        SingleOutputStreamOperator<JSONObject> jsonObjDS = kafkaStrDS.map(JSON::parseObject);

        // 3.2 过滤首页(home)和商品详情页(good_detail)
        SingleOutputStreamOperator<JSONObject> filterDS = jsonObjDS.filter(
                new FilterFunction<JSONObject>() {
                    @Override
                    public boolean filter(JSONObject jsonObj) {
                        String pageId = jsonObj.getJSONObject("page").getString("page_id");
                        return "home".equals(pageId) || "good_detail".equals(pageId);
                    }
                }
        );

        // === 4. 时间语义处理 ===
        SingleOutputStreamOperator<JSONObject> withWatermarkDS = filterDS.assignTimestampsAndWatermarks(
                WatermarkStrategy.<JSONObject>forMonotonousTimestamps()
                        .withTimestampAssigner(
                                (jsonObj, recordTimestamp) -> jsonObj.getLong("ts")  // 提取事件时间戳
                        )
        );

        // === 5. UV统计核心逻辑 ===
        // 5.1 按设备ID(mid)分组
        KeyedStream<JSONObject, String> keyedDS = withWatermarkDS.keyBy(
                jsonObj -> jsonObj.getJSONObject("common").getString("mid")
        );

        // 5.2 使用状态编程实现UV去重
        SingleOutputStreamOperator<TrafficHomeDetailPageViewBean> beanDS = keyedDS.process(
                new KeyedProcessFunction<String, JSONObject, TrafficHomeDetailPageViewBean>() {
                    // 定义状态：记录设备最后一次访问首页/详情页的日期
                    private ValueState<String> homeLastVisitDateState;
                    private ValueState<String> detailLastVisitDateState;

                    @Override
                    public void open(Configuration parameters) {
                        // 首页访问状态（TTL设置为1天自动过期）
                        ValueStateDescriptor<String> homeStateDesc =
                                new ValueStateDescriptor<>("homeLastVisitDateState", String.class);
                        homeStateDesc.enableTimeToLive(
                                StateTtlConfig.newBuilder(Time.days(1)).build()
                        );
                        homeLastVisitDateState = getRuntimeContext().getState(homeStateDesc);

                        // 详情页访问状态（TTL设置为1天自动过期）
                        ValueStateDescriptor<String> detailStateDesc =
                                new ValueStateDescriptor<>("detailLastVisitDateState", String.class);
                        detailStateDesc.enableTimeToLive(
                                StateTtlConfig.newBuilder(Time.days(1)).build()
                        );
                        detailLastVisitDateState = getRuntimeContext().getState(detailStateDesc);
                    }

                    @Override
                    public void processElement(JSONObject jsonObj, Context ctx,
                                               Collector<TrafficHomeDetailPageViewBean> out) throws Exception {

                        String pageId = jsonObj.getJSONObject("page").getString("page_id");
                        Long ts = jsonObj.getLong("ts");
                        String curVisitDate = DateFormatUtil.tsToDate(ts);  // 转换为yyyy-MM-dd格式

                        long homeUvCt = 0L;
                        long detailUvCt = 0L;

                        // 首页UV判断逻辑
                        if ("home".equals(pageId)) {
                            String lastVisitDate = homeLastVisitDateState.value();
                            if (StringUtils.isEmpty(lastVisitDate)  || !lastVisitDate.equals(curVisitDate))  {
                                homeUvCt = 1L;
                                homeLastVisitDateState.update(curVisitDate);  // 更新状态
                            }
                        }
                        // 详情页UV判断逻辑
                        else if ("good_detail".equals(pageId)) {
                            String lastVisitDate = detailLastVisitDateState.value();
                            if (StringUtils.isEmpty(lastVisitDate)  || !lastVisitDate.equals(curVisitDate))  {
                                detailUvCt = 1L;
                                detailLastVisitDateState.update(curVisitDate);  // 更新状态
                            }
                        }

                        // 仅当有UV计数时才下发数据
                        if (homeUvCt > 0 || detailUvCt > 0) {
                            out.collect(new  TrafficHomeDetailPageViewBean(
                                    "", "", "", homeUvCt, detailUvCt, ts
                            ));
                        }
                    }
                }
        );

        // === 6. 窗口聚合 ===
        // 6.1 开窗（10秒滚动事件时间窗口）
        AllWindowedStream<TrafficHomeDetailPageViewBean, TimeWindow> windowDS = beanDS
                .windowAll(TumblingEventTimeWindows.of(
                        org.apache.flink.streaming.api.windowing.time.Time.seconds(10)
                ));

        // 6.2 聚合计算（UV求和+补充窗口时间信息）
        SingleOutputStreamOperator<TrafficHomeDetailPageViewBean> reduceDS = windowDS.reduce(
                // 增量聚合函数
                new ReduceFunction<TrafficHomeDetailPageViewBean>() {
                    @Override
                    public TrafficHomeDetailPageViewBean reduce(TrafficHomeDetailPageViewBean v1, TrafficHomeDetailPageViewBean v2) {
                        v1.setHomeUvCt(v1.getHomeUvCt()  + v2.getHomeUvCt());
                        v1.setGoodDetailUvCt(v1.getGoodDetailUvCt()  + v2.getGoodDetailUvCt());
                        return v1;
                    }
                },
                // 全窗口函数
                new ProcessAllWindowFunction<TrafficHomeDetailPageViewBean,
                        TrafficHomeDetailPageViewBean, TimeWindow>() {
                    @Override
                    public void process(Context context, Iterable<TrafficHomeDetailPageViewBean> elements,
                                        Collector<TrafficHomeDetailPageViewBean> out) {

                        TrafficHomeDetailPageViewBean result = elements.iterator().next();
                        TimeWindow window = context.window();

                        // 补充窗口时间信息
                        result.setStt(DateFormatUtil.tsToDateTime(window.getStart()));  // 窗口起始时间
                        result.setEdt(DateFormatUtil.tsToDateTime(window.getEnd()));    // 窗口结束时间
                        result.setCurDate(DateFormatUtil.tsToDate(window.getStart()));  // 窗口日期

                        out.collect(result);
                    }
                }
        );

        // === 7. 结果输出 ===
        // 7.1 转换为JSON字符串
        SingleOutputStreamOperator<String> jsonMap = reduceDS
                .map(new BeanToJsonStrMapFunction<>());

        // 7.2 打印调试（生产环境可移除）
        jsonMap.print();

        // 7.3 写入Doris
        jsonMap.sinkTo(
                FlinkSinkUtil.getDorisSink("dws_traffic_home_detail_page_view_window")
        );

        // === 8. 启动作业 ===
        env.execute("DwsTrafficHomeDetailPageViewWindow");
    }
}