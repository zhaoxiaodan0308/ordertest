package com.phone.analystic.mr.nm;

import com.phone.Util.JdbcUtil;
import com.phone.Util.TimeUtil;
import com.phone.analystic.modle.StatsUserDimension;
import com.phone.analystic.modle.base.DateDimension;
import com.phone.analystic.modle.value.map.TimeOutputValue;
import com.phone.analystic.modle.value.reduce.OutputWritable;
import com.phone.analystic.mr.OutputToMySqlFormat;
import com.phone.analystic.mr.service.impl.IDimensionImpl;
import com.phone.common.DateEnum;
import com.phone.common.GlobalConstants;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;


/**
 * FileName: EtlToHdfsRunner
 * Author: zhao
 * Date: 2018/9/19 19:52
 * Description:统计用户基本信息表的新增用户、总用户
 * 统计浏览器分析表的新增用户、总用户
 * 筛选的是lanuch事件 e_l
 * History:
 * <author> <time> <version> <desc>
 * 作者姓名 修改时间 版本号 描述
 */
public class NewMembersRunner implements Tool {
    private static Logger logger = Logger.getLogger(NewMembersRunner.class);
    Configuration conf = new Configuration();

    @Override
    public int run(String[] args) throws Exception {
        Configuration conf = this.getConf();
        //设置输入参数的时间
        this.handleArgs(conf, args);

        Job job = Job.getInstance(conf, "newMember to mysql");

        //设置运行的类
        job.setJarByClass(NewMembersRunner.class);

        //设置map和输出
        job.setMapperClass(NewMembersMapper.class);
        job.setMapOutputKeyClass(StatsUserDimension.class);
        job.setMapOutputValueClass(TimeOutputValue.class);

        //设置reduce和输出
        job.setReducerClass(NewMembersReducer.class);
        job.setMapOutputKeyClass(StatsUserDimension.class);
        job.setOutputValueClass(OutputWritable.class);

        //输入文件设置
        handleInputOutput(job);

        //设置redcuce的输出格式类
        job.setOutputFormatClass(OutputToMySqlFormat.class);

//        return job.waitForCompletion(true) ? 1 : 0;
        if (job.waitForCompletion(true)) {

            computeTotalNewMembers(job);

            return 0;
        } else {
            return 1;
        }
    }

    /**
     * 获取总会员的人数=同一个维度，前一天的总用户+当天新增用户
     * 1、获取运行日期当天和前一天的时间维度，并获取其对应的时间维度id，判断id是否大于0。
     * 2、根据时间维度的id获取前天的总用户和当天的新增用户。
     * 3、更新新增总用户
     * 如果是计算月维度的总会员，那么就是拿上个月这个维度的总会员 + 这个月维度下的新增会员作为这个月维度下的总会员。
     * 新增用户维度和浏览器维度
     *
     * @param job
     */
    private void computeTotalNewMembers(Job job) {
        //获取表连接
        Configuration conf = null;
        Connection conn = null;

        try {
            conf = job.getConfiguration();
            conn = JdbcUtil.getConn();

            //-------------------------日维度--------------------------
            //当前的日期
            long today = TimeUtil.parseString2Long(conf.get(GlobalConstants.RUNNING_DATE));
            long yesterdaty = today - 86400000;


            IDimensionImpl iDimension = new IDimensionImpl();
            //获取当天时间维度的id
            DateDimension dateDimensionT = DateDimension.buildDate(today, DateEnum.DAY);
            int idT = iDimension.getDiemnsionIdByObject(dateDimensionT);

            //获取昨天时间维度的id
            DateDimension dateDimensionY = DateDimension.buildDate(yesterdaty, DateEnum.DAY);
            int idY = iDimension.getDiemnsionIdByObject(dateDimensionY);

            //更新
            update(idY, idT, conn);

            //-------------------------月维度--------------------------
            //当前的日期

            long lastMonth = TimeUtil.getlastMonth(today);


            //获取当天时间维度的id
            DateDimension dateDimensionMonth = DateDimension.buildDate(today, DateEnum.MONTH);
            int idMonth = iDimension.getDiemnsionIdByObject(dateDimensionMonth);

            //获取昨天时间维度的id
            DateDimension dateDimensionLastMonth = DateDimension.buildDate(lastMonth, DateEnum.MONTH);
            int idLastMonth = iDimension.getDiemnsionIdByObject(dateDimensionLastMonth);

            //更新
            update(idMonth, idLastMonth, conn);


        } catch (Exception e) {
            logger.error("更新总用户失败", e);
        } finally {
            JdbcUtil.close(conn, null, null);
        }

    }

    public void update(int idY, int idT, Connection conn) {

        PreparedStatement ps = null;
        ResultSet rs = null;

        try {
            //----------------------------新增用户------------------------------
            //获取昨天新增总用户
            Map<String, Integer> map = new HashMap<String, Integer>();
            if (idY > 0) {
                ps = conn.prepareStatement(conf.get("total_members_select"));
                ps.setInt(1, idY);
                rs = ps.executeQuery();

                while (rs.next()) {
                    int platform_dimension_id = rs.getInt(1);
                    int total_members = rs.getInt(2);

                    String key = String.valueOf(platform_dimension_id);

                    map.put(key, total_members);
                }
            }

            //获取今天新增总用户
            if (idT > 0) {
                ps = conn.prepareStatement(conf.get("new_members_select"));
                ps.setInt(1, idT);
                rs = ps.executeQuery();

                while (rs.next()) {
                    int platform_dimension_id = rs.getInt(1);
                    int new_members = rs.getInt(2);

                    String key = String.valueOf(platform_dimension_id);
                    //存储
                    if (map.containsKey(key)) {
                        Integer total_members = map.get(key);
                        new_members += total_members;
                    }
                    //覆盖
                    map.put(key, new_members);

                }
            }

            //更新map中的数据
            ps = conn.prepareStatement(conf.get("total_members_update"));

            for (Map.Entry<String, Integer> en : map.entrySet()) {
                ps.setInt(1, idT);
                ps.setInt(2, Integer.parseInt(en.getKey()));
                ps.setInt(3, en.getValue());
                ps.setString(4, conf.get(GlobalConstants.RUNNING_DATE));
                ps.setInt(5, en.getValue());

                ps.addBatch();
            }

            ps.executeBatch();  //批量执行

            //----------------------------浏览器------------------------------
            map.clear();
            //获取昨天新增总用户
            if (idY > 0) {
                ps = conn.prepareStatement(conf.get("browser_total_members_select"));
                ps.setInt(1, idY);
                rs = ps.executeQuery();

                while (rs.next()) {
                    int platform_dimension_id = rs.getInt(1);
                    int browser_dimension_id = rs.getInt(2);
                    int total_members = rs.getInt(3);

                    String key = platform_dimension_id + "_" + browser_dimension_id;

                    map.put(key, total_members);
                }
            }

            //获取今天新增总用户
            if (idT > 0) {
                ps = conn.prepareStatement(conf.get("browser_new_members_select"));
                ps.setInt(1, idT);
                rs = ps.executeQuery();

                while (rs.next()) {
                    int platform_dimension_id = rs.getInt(1);
                    int browser_dimension_id = rs.getInt(2);
                    int new_members = rs.getInt(3);

                    String key = platform_dimension_id + "_" + browser_dimension_id;
                    //存储
                    if (map.containsKey(key)) {
                        Integer total_members = map.get(key);
                        new_members += total_members;
                    }
                    //覆盖
                    map.put(key, new_members);

                }
            }

            //更新map中的数据
            ps = conn.prepareStatement(conf.get("browser_total_members_update"));

            for (Map.Entry<String, Integer> en : map.entrySet()) {
                String[] split = en.getKey().split("_");
                ps.setInt(1, idT);
                ps.setInt(2, Integer.parseInt(split[0]));
                ps.setInt(3, Integer.parseInt(split[1]));
                ps.setInt(4, en.getValue());
                ps.setString(5, conf.get(GlobalConstants.RUNNING_DATE));
                ps.setInt(6, en.getValue());

                ps.addBatch();
            }

            ps.executeBatch();  //批量执行
        } catch (SQLException e) {
            logger.error("更新数据失败",e);
        } finally {
            JdbcUtil.close(null, ps, rs);
        }


    }


    @Override
    public void setConf(Configuration conf) {
        conf.addResource("output_mapping.xml");
        conf.addResource("output_writer.xml");
        conf.addResource("other_mapping.xml");

        this.conf = conf;
    }

    @Override
    public Configuration getConf() {
        return conf;
    }

    //解析获取的时间
    private void handleArgs(Configuration conf, String[] args) {
        String date = null;
        if (args.length > 0) {
            for (int i = 0; i < args.length; i++) {
                if (args[i].equals("-d")) {
                    if (i + 1 <= args.length - 1) {
                        date = args[i + 1];
                        break;
                    }
                }
            }
            if (StringUtils.isEmpty(date)) {
                throw new RuntimeException("获取时间参数异常");
            } else {
                conf.set(GlobalConstants.RUNNING_DATE, date);
            }
        }
    }


    //设置输入输出文件
    private void handleInputOutput(Job job) {
        Configuration conf = job.getConfiguration();
        String[] strings = conf.get(GlobalConstants.RUNNING_DATE).split("-");
        String m = strings[1];
        String d = strings[2];
        try {
            FileSystem fs = FileSystem.get(conf);
            Path inpath = new Path("/ods/" + m + "/" + d);

            if (fs.exists(inpath)) {
                FileInputFormat.setInputPaths(job, inpath);
            } else {
                throw new RuntimeException("输入路径不存储在.inpath:" + inpath.toString());
            }


        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static void main(String[] args) {
        try {
            ToolRunner.run(new Configuration(), new NewMembersRunner(), args);
        } catch (Exception e) {
            logger.error("运行异常");
        }
    }
}
