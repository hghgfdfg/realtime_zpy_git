package com.zpy.stream.realtime.v1.app.dws;

import com.v1.constant.Constant;
import com.v1.function.KeywordUDTF;
import com.v1.utils.SQLUtil;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * DWS层：搜索关键词统计
 * 功能：统计每2秒窗口内用户通过搜索进入商品页的关键词及其出现次数
 * 数据流：Kafka(dwd层页面日志) → Flink SQL处理 → Doris(dws层聚合结果)
 */
public class DwsTrafficSourceKeywordPageViewWindow {
    public static void main(String[] args) throws Exception {
        // === 1. 初始化Flink环境 ===
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);   // 生产环境建议根据集群资源调整
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
        env.enableCheckpointing(5000L);   // 5秒一次检查点

        // === 2. 注册自定义函数 ===
        // 注册IK分词器UDTF函数，用于将搜索关键词拆分为单个词语
        tableEnv.createTemporarySystemFunction("ik_analyze",  KeywordUDTF.class);

        // === 3. 创建Kafka源表 ===
        // 定义从Kafka读取页面日志的表结构
        tableEnv.executeSql("create  table page_log(\n" +
                "     common map<string,string>,\n" +
                "     page map<string,string>,\n" +
                "     ts TIMESTAMP(3) METADATA FROM 'timestamp', \n" +
                "     WATERMARK FOR ts AS ts - INTERVAL '3' SECOND \n" +
                ")" + SQLUtil.getKafkaDDL(Constant.TOPIC_DWD_TRAFFIC_PAGE,"dws_traffic_source_keyword_page_view_window"));
        // tableEnv.executeSql("select  * from page_log").print();

        // === 4. 提取搜索关键词 ===
        // 从页面日志中筛选出来自搜索页(search)且类型为关键词(keyword)的记录
        Table searchTable = tableEnv.sqlQuery("select  \n" +
                "   page['item']  fullword,\n" +  // 获取完整的搜索关键词
                "   ts \n" +                      // 时间戳
                " from page_log\n" +
                " where page['last_page_id'] = 'search' " +  // 上一页是搜索页
                " and page['item_type'] ='keyword' " +       // 项目类型是关键词
                " and page['item'] is not null");            // 关键词不为空
        tableEnv.createTemporaryView("search_table",searchTable);
        // searchTable.execute().print();

        // === 5. 关键词分词处理 ===
        // 使用IK分词器将完整的搜索关键词拆分为单个词语
        Table splitTable = tableEnv.sqlQuery("SELECT  keyword,ts FROM search_table,\n" +
                "LATERAL TABLE(ik_analyze(fullword)) t(keyword)");
        tableEnv.createTemporaryView("split_table",splitTable);
        // tableEnv.executeSql("select  * from split_table").print();

        // === 6. 窗口聚合统计 ===
        // 每2秒统计一次各关键词的出现次数，并补充窗口时间信息
        Table resTable = tableEnv.sqlQuery("SELECT  \n" +
                "  date_format(window_start, 'yyyy-MM-dd HH:mm:ss') stt,\n" +  // 窗口开始时间
                "  date_format(window_end, 'yyyy-MM-dd HH:mm:ss') edt,\n" +    // 窗口结束时间
                "  date_format(window_start, 'yyyy-MM-dd') cur_date,\n" +      // 窗口日期
                "  keyword,\n" +                                               // 关键词
                "  count(*) keyword_count\n" +                                 // 关键词出现次数
                "  FROM TABLE(\n" +
                "  TUMBLE(TABLE split_table, DESCRIPTOR(ts), INTERVAL '2' second))\n" +  // 2秒滚动窗口
                "  GROUP BY window_start, window_end, keyword");
        // 调试用：打印聚合结果
        // resTable.execute().print();

        // === 7. 创建Doris结果表 ===
        tableEnv.executeSql("create  table dws_traffic_source_keyword_page_view_window(" +
                "  stt string, " +  // 窗口开始时间，格式：2023-07-11 14:14:14
                "  edt string, " +  // 窗口结束时间
                "  cur_date string, " +  // 当前日期
                "  keyword string, " +   // 关键词
                "  keyword_count bigint " +  // 关键词出现次数
                ")with(" +
                " 'connector' = 'doris'," +  // 使用Doris连接器
                " 'fenodes' = '" + Constant.DORIS_FE_NODES + "'," +  // Doris FE节点地址
                "  'table.identifier'  = '" + Constant.DORIS_DATABASE + ".dws_traffic_source_keyword_page_view_window'," +  // Doris表名
                "  'username' = 'admin'," +  // 用户名
                "  'password' = 'zh1028,./', " +  // 密码
                "  'sink.properties.format'  = 'json', " +  // 使用JSON格式
                "  'sink.buffer-count'  = '4', " +  // 缓冲区数量
                "  'sink.buffer-size'  = '4086'," +  // 缓冲区大小
                "  'sink.enable-2pc'  = 'false', " + // 测试阶段关闭两阶段提交
                "  'sink.properties.read_json_by_line'  = 'true' " +  // 按行读取JSON
                ")");

        // === 8. 结果写入Doris ===
        resTable.executeInsert("dws_traffic_source_keyword_page_view_window");

        // === 9. 启动作业 ===
        env.execute("DwsTrafficSourceKeywordPageViewWindow");
    }
}