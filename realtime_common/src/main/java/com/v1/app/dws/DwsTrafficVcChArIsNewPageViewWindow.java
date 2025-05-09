package com.v1.app.dws;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.v1.bean.TrafficVcChArIsNewPageViewBean;
import com.v1.function.BeanToJsonStrMapFunction;
import com.v1.utils.DateFormatUtil;
import com.v1.utils.FlinkSinkUtil;
import com.v1.utils.FlinkSourceUtil;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

/**
 * DWS层：多维度页面访问统计
 * 功能：按版本(vc)、渠道(ch)、地区(ar)和新老用户(is_new)维度统计每10秒窗口内的页面访问指标
 * 数据流：Kafka(dwd层页面日志) → Flink实时处理 → Doris(dws层聚合结果)
 * 主要指标：
 *   - UV(独立访客数)
 *   - SV(会话次数)
 *   - PV(页面访问量)
 *   - 页面停留总时长
 */
public class DwsTrafficVcChArIsNewPageViewWindow {
    public static void main(String[] args) throws Exception {
        // === 1. 初始化Flink环境 ===
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);   // 生产环境建议根据集群资源调整
        env.enableCheckpointing(5000L,  CheckpointingMode.EXACTLY_ONCE);  // 5秒一次精确一次检查点
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3,  3000L));  // 失败重启策略

        // === 2. 数据源配置 ===
        KafkaSource<String> kafkaSource = FlinkSourceUtil.getKafkaSource(
                "dwd_traffic_page_pengyu_zhu",  // 原始页面日志Topic
                "dws_traffic_vc_ch_ar_is_new_page_view_window"  // 消费者组
        );
        DataStreamSource<String> kafkaStrDS = env.fromSource(
                kafkaSource,
                WatermarkStrategy.noWatermarks(),
                "Kafka_Source"
        );

        // === 3. 数据预处理 ===
        // 3.1 JSON字符串转对象
        SingleOutputStreamOperator<JSONObject> jsonObjDS = kafkaStrDS.map(JSON::parseObject);

        // 3.2 按设备ID(mid)分组
        KeyedStream<JSONObject, String> midKeyedDS = jsonObjDS.keyBy(
                jsonObj -> jsonObj.getJSONObject("common").getString("mid")
        );

        // === 4. 核心指标计算 ===
        SingleOutputStreamOperator<TrafficVcChArIsNewPageViewBean> beanDS = midKeyedDS.map(
                new RichMapFunction<JSONObject, TrafficVcChArIsNewPageViewBean>() {
                    // 定义状态：记录设备最后一次访问的日期
                    private ValueState<String> lastVisitDateState;

                    @Override
                    public void open(Configuration parameters) {
                        // 初始化状态，设置1天TTL自动过期
                        ValueStateDescriptor<String> valueStateDescriptor =
                                new ValueStateDescriptor<>("lastVisitDateState", String.class);
                        valueStateDescriptor.enableTimeToLive(
                                StateTtlConfig.newBuilder(Time.days(1)).build()
                        );
                        lastVisitDateState = getRuntimeContext().getState(valueStateDescriptor);
                    }

                    @Override
                    public TrafficVcChArIsNewPageViewBean map(JSONObject jsonObj) throws Exception {
                        JSONObject commonJsonObj = jsonObj.getJSONObject("common");
                        JSONObject pageJsonObj = jsonObj.getJSONObject("page");

                        // UV计算逻辑：判断是否为当天首次访问
                        String lastVisitDate = lastVisitDateState.value();
                        Long ts = jsonObj.getLong("ts");
                        String curVisitDate = DateFormatUtil.tsToDate(ts);
                        long uvCt = 0L;
                        if (StringUtils.isEmpty(lastVisitDate)  || !lastVisitDate.equals(curVisitDate))  {
                            uvCt = 1L;
                            lastVisitDateState.update(curVisitDate);   // 更新状态
                        }

                        // SV计算逻辑：判断是否为会话开始(无上一页)
                        String lastPageId = pageJsonObj.getString("last_page_id");
                        Long svCt = StringUtils.isEmpty(lastPageId)  ? 1L : 0L;

                        // 构造指标Bean
                        return new TrafficVcChArIsNewPageViewBean(
                                "",  // stt(窗口开始时间，后续补充)
                                "",  // edt(窗口结束时间，后续补充)
                                "",  // cur_date(当前日期，后续补充)
                                commonJsonObj.getString("vc"),   // 版本
                                commonJsonObj.getString("ch"),   // 渠道
                                commonJsonObj.getString("ar"),   // 地区
                                commonJsonObj.getString("is_new"),   // 新老用户标识
                                uvCt,  // 独立访客数
                                svCt,  // 会话次数
                                1L,  // 页面访问量(每次访问记为1)
                                pageJsonObj.getLong("during_time"),   // 页面停留时长
                                ts  // 时间戳
                        );
                    }
                }
        );

        // === 5. 时间语义处理 ===
        SingleOutputStreamOperator<TrafficVcChArIsNewPageViewBean> withWatermarkDS = beanDS.assignTimestampsAndWatermarks(
                WatermarkStrategy
                        .<TrafficVcChArIsNewPageViewBean>forMonotonousTimestamps()
                        .withTimestampAssigner(
                                (bean, recordTimestamp) -> bean.getTs()   // 提取事件时间戳
                        )
        );

        // === 6. 按维度分组 ===
        // 按版本(vc)、渠道(ch)、地区(ar)和新老用户(is_new)四个维度分组
        KeyedStream<TrafficVcChArIsNewPageViewBean, Tuple4<String, String, String, String>> dimKeyedDS = withWatermarkDS.keyBy(
                new KeySelector<TrafficVcChArIsNewPageViewBean, Tuple4<String, String, String, String>>() {
                    @Override
                    public Tuple4<String, String, String, String> getKey(TrafficVcChArIsNewPageViewBean bean) {
                        return Tuple4.of(
                                bean.getVc(),   // 版本
                                bean.getCh(),   // 渠道
                                bean.getAr(),   // 地区
                                bean.getIsNew()   // 新老用户标识
                        );
                    }
                }
        );

        // === 7. 窗口聚合 ===
        // 7.1 开窗(10秒滚动事件时间窗口)
        WindowedStream<TrafficVcChArIsNewPageViewBean, Tuple4<String, String, String, String>, TimeWindow> windowDS =
                dimKeyedDS.window(TumblingEventTimeWindows.of(
                        org.apache.flink.streaming.api.windowing.time.Time.seconds(10)
                ));

        // 7.2 聚合计算
        SingleOutputStreamOperator<TrafficVcChArIsNewPageViewBean> reduceDS = windowDS.reduce(
                // 增量聚合函数：累加各指标值
                new ReduceFunction<TrafficVcChArIsNewPageViewBean>() {
                    @Override
                    public TrafficVcChArIsNewPageViewBean reduce(TrafficVcChArIsNewPageViewBean value1,
                                                                 TrafficVcChArIsNewPageViewBean value2) {
                        value1.setPvCt(value1.getPvCt()  + value2.getPvCt());   // 累加PV
                        value1.setUvCt(value1.getUvCt()  + value2.getUvCt());   // 累加UV
                        value1.setSvCt(value1.getSvCt()  + value2.getSvCt());   // 累加SV
                        value1.setDurSum(value1.getDurSum()  + value2.getDurSum());   // 累加停留时长
                        return value1;
                    }
                },
                // 全窗口函数：补充窗口时间信息
                new WindowFunction<TrafficVcChArIsNewPageViewBean,
                        TrafficVcChArIsNewPageViewBean,
                        Tuple4<String, String, String, String>,
                        TimeWindow>() {
                    @Override
                    public void apply(Tuple4<String, String, String, String> key,
                                      TimeWindow window,
                                      Iterable<TrafficVcChArIsNewPageViewBean> input,
                                      Collector<TrafficVcChArIsNewPageViewBean> out) {
                        TrafficVcChArIsNewPageViewBean pageViewBean = input.iterator().next();
                        // 补充窗口时间信息
                        pageViewBean.setStt(DateFormatUtil.tsToDateTime(window.getStart()));   // 窗口开始时间
                        pageViewBean.setEdt(DateFormatUtil.tsToDateTime(window.getEnd()));   // 窗口结束时间
                        pageViewBean.setCur_date(DateFormatUtil.tsToDate(window.getStart()));   // 窗口日期
                        out.collect(pageViewBean);
                    }
                }
        );

        // === 8. 结果输出 ===
        // 8.1 转换为JSON字符串
        SingleOutputStreamOperator<String> jsonMap = reduceDS
                .map(new BeanToJsonStrMapFunction<>());

        // 8.2 打印调试(生产环境可移除)
        jsonMap.print();

        // 8.3 写入Doris
        jsonMap.sinkTo(
                FlinkSinkUtil.getDorisSink("dws_traffic_vc_ch_ar_is_new_page_view_window")
        );

        // === 9. 启动作业 ===
        env.execute("DwsTrafficVcChArIsNewPageViewWindow");
    }
}