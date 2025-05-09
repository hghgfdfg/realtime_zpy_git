package com.v1.app.dwd;

import com.v1.constant.Constant;
import com.v1.utils.SQLUtil;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.time.Duration;

/**
 * DWD层订单取消明细处理程序
 * 功能：处理订单取消数据，关联订单明细数据，生成完整的订单取消明细记录
 *主要任务:
 *关联订单明细表、订单表、订单明细活动关联表、订单明细优惠券关联表四张事实业务表的insert操作，
 * 形成下单明细表，写入 Kafka 对应主题。
 * 主要流程：
 * 1. 从Kafka读取订单原始数据
 * 2. 过滤出取消状态的订单(状态码1001或1003)
 * 3. 关联DWD层订单明细表获取完整订单信息
 * 4. 将处理后的数据写入Kafka的DWD层主题
 */

public class DwdTradeOrderCancelDetail {
    public static void main(String[] args) throws Exception {
        // 1. 创建流处理环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 设置并行度为4
        env.setParallelism(4);

        // 创建Table API环境
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        // 启用检查点，每5秒一次，使用EXACTLY_ONCE语义
        env.enableCheckpointing(5000L, CheckpointingMode.EXACTLY_ONCE);

        // 设置状态保留时间为30分钟5秒(用于join操作的状态保留)
        tableEnv.getConfig().setIdleStateRetention(Duration.ofSeconds(30 * 60 + 5));

        // 2. 创建Kafka源表(订单原始数据)
        tableEnv.executeSql("CREATE TABLE topic_db (\n" +
                "  after MAP<string, string>, \n" +
                "  source MAP<string, string>, \n" +
                "  `op` string, \n" +
                "  ts_ms bigint " +
                ")" + SQLUtil.getKafkaDDL(Constant.TOPIC_DB, Constant.TOPIC_DWD_INTERACTION_COMMENT_INFO));
        // 3. 创建HBase维表(字典表)
        tableEnv.executeSql("CREATE TABLE base_dic (\n" +
                " dic_code string,\n" +
                " info ROW<dic_name string>,\n" +
                " PRIMARY KEY (dic_code) NOT ENFORCED\n" +
                ") " + SQLUtil.getHBaseDDL("dim_base_dic")
        );

        // 4. 提取取消订单数据
        // 从topic_db中筛选order_info表的取消状态订单(状态码1001或1003)
        Table orderCancel = tableEnv.sqlQuery("select " +
                " `after`['id'] as id, " +
                " `after`['operate_time'] as operate_time, " +
                " `ts_ms` " +
                " from topic_db " +
                " where source['table'] = 'order_info' " +
                " and `op` = 'r' " +
                " and `after`['order_status'] = '1001' " +  // 订单状态1001(已取消)
                " or `after`['order_status'] = '1003' ");   // 或1003(已退款)
        tableEnv.createTemporaryView("order_cancel", orderCancel);

        // 创建取消订单临时视图
        tableEnv.createTemporaryView("order_cancel", orderCancel);

        // 5. 创建订单明细事实表
        tableEnv.executeSql(
                "create table dwd_trade_order_detail(" +
                        "  id string," +
                        "  order_id string," +
                        "  user_id string," +
                        "  sku_id string," +
                        "  sku_name string," +
                        "  province_id string," +
                        "  activity_id string," +
                        "  activity_rule_id string," +
                        "  coupon_id string," +
                        "  date_id string," +
                        "  create_time string," +
                        "  sku_num string," +
                        "  split_original_amount string," +
                        "  split_activity_amount string," +
                        "  split_coupon_amount string," +
                        "  split_total_amount string," +
                        "  ts_ms bigint " +
                        "  )" + SQLUtil.getKafkaDDL(Constant.TOPIC_DWD_TRADE_ORDER_DETAIL,Constant.TOPIC_DWD_TRADE_ORDER_CANCEL));

        // 6. 关联订单明细和取消订单
        Table result = tableEnv.sqlQuery(
                "select  " +
                        "  od.id," +
                        "  od.order_id," +
                        "  od.user_id," +
                        "  od.sku_id," +
                        "  od.sku_name," +
                        "  od.province_id," +
                        "  od.activity_id," +
                        "  od.activity_rule_id," +
                        "  od.coupon_id," +
                        "  date_format(TO_TIMESTAMP(FROM_UNIXTIME(CAST(od.create_time AS BIGINT) / 1000)), 'yyyy-MM-dd') date_id, " +
                        "  oc.operate_time," +
                        "  od.sku_num," +
                        "  od.split_original_amount," +
                        "  od.split_activity_amount," +
                        "  od.split_coupon_amount," +
                        "  od.split_total_amount," +
                        "  oc.ts_ms " +
                        "  from dwd_trade_order_detail od " +   // 订单明细表
                        "  join order_cancel oc " +         // 关联取消订单表
                        "  on od.order_id = oc.id ");       // 关联条件:订单ID相等
//        result.execute().print();


        // 7. 创建Kafka结果表(订单取消明细)
        tableEnv.executeSql(
                "create table "+Constant.TOPIC_DWD_TRADE_ORDER_CANCEL+"(" +
                        "  id string," +
                        "  order_id string," +
                        "  user_id string," +
                        "  sku_id string," +
                        "  sku_name string," +
                        "  province_id string," +
                        "  activity_id string," +
                        "  activity_rule_id string," +
                        "  coupon_id string," +
                        "  date_id string," +
                        "  cancel_time string," +
                        "  sku_num string," +
                        "  split_original_amount string," +
                        "  split_activity_amount string," +
                        "  split_coupon_amount string," +
                        "  split_total_amount string," +
                        "  ts_ms bigint ," +
                        "  PRIMARY KEY (id) NOT ENFORCED " +
                        "  )" + SQLUtil.getUpsertKafkaDDL(Constant.TOPIC_DWD_TRADE_ORDER_CANCEL));
        result.executeInsert(Constant.TOPIC_DWD_TRADE_ORDER_CANCEL);

        // 9. 执行作业
        env.execute("DwdTradeOrderCancelDetail");
    }
}