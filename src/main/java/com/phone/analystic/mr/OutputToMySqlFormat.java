package com.phone.analystic.mr;

import com.phone.Util.JdbcUtil;
import com.phone.analystic.modle.StatsBaseDimension;
import com.phone.analystic.modle.value.StatsOutpuValue;
import com.phone.analystic.mr.service.IDimension;
import com.phone.analystic.mr.service.impl.IDimensionImpl;
import com.phone.common.KpiType;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;

/**
 * FileName: OutputToMySqlFormat
 * Author: zhao
 * Date: 2018/9/24 13:47
 * Description:
 * History:
 * <author> <time> <version> <desc>
 * 作者姓名 修改时间 版本号 描述
 */
public class OutputToMySqlFormat extends OutputFormat<StatsBaseDimension, StatsOutpuValue> {
    private static Logger logger = Logger.getLogger(OutputToMySqlFormat.class);

    /**
     * 获取输出记录
     *
     * @param taskAttemptContext
     * @return
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    public RecordWriter<StatsBaseDimension, StatsOutpuValue> getRecordWriter(TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
        Connection conn = JdbcUtil.getConn();
        Configuration conf = taskAttemptContext.getConfiguration();
        IDimension iDimension = new IDimensionImpl();

        return new OutputToMysqlRecordWritter(conf, conn, iDimension);
    }

    @Override
    public void checkOutputSpecs(JobContext jobContext) throws IOException, InterruptedException {
        //检测输出空间
    }

    @Override
    public OutputCommitter getOutputCommitter(TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
        return new FileOutputCommitter(null, taskAttemptContext);
    }

    /**
     * 用于封装写出记录到mysql的信息
     */
    public static class OutputToMysqlRecordWritter extends RecordWriter<StatsBaseDimension, StatsOutpuValue> {
        Configuration conf = null;
        Connection conn = null;
        IDimension iDimension = null;
        //存储kpi-ps
        private Map<KpiType, PreparedStatement> map = new HashMap<KpiType, PreparedStatement>();
        //存储kpi-对应的输出sql
        private Map<KpiType, Integer> batch = new HashMap<KpiType, Integer>();

        public OutputToMysqlRecordWritter(Configuration conf, Connection conn, IDimension iDimension) {
            this.conf = conf;
            this.conn = conn;
            this.iDimension = iDimension;
        }

        /**
         * 写
         *
         * @param key
         * @param value
         * @throws IOException
         * @throws InterruptedException
         */
        @Override
        public void write(StatsBaseDimension key, StatsOutpuValue value) throws IOException, InterruptedException {
            //获取kpi
            KpiType kpi = value.getKpi();
            PreparedStatement ps = null;

            try {
                //获取ps
                if (map.containsKey(kpi)) {
                    ps = map.get(kpi);
                } else {
                    ps = conn.prepareStatement(conf.get(kpi.kpiName));
                    map.put(kpi, ps);  //将新增加的ps存储到map中
                }

                int count = 1;
                this.batch.put(kpi, count);
                count++;

                //为ps赋值准备
                String calssName = conf.get("writer_" + kpi.kpiName);

                //com.phone.analystic.mr.nu.NewUserOutputWriter
                Class<?> classz = Class.forName(calssName); //将报名+类名转换成类
                IOutputWritter writer = (IOutputWritter) classz.newInstance();
                //调用IOutputWritter中的output方法
                writer.ouput(conf, key, value, ps, iDimension);


                //有问题
                //对赋值好的ps进行执行
                if (batch.size() % 50 == 0) {  //有50个ps执行
                    ps.executeBatch();  //批量执行
//                    this.conn.commit(); //提交批处理执行
                    batch.remove(kpi); //将执行完的ps移除掉
                }

            } catch (Exception e) {
                logger.error("更新最数据异常",e);
            }
        }

        @Override
        public void close(TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
            try {
                for (Map.Entry<KpiType, PreparedStatement> en : map.entrySet()) {
                    en.getValue().executeBatch(); //将剩余的ps进行执行
//                    this.conn.commit();
                }
            } catch (SQLException e) {
                logger.error("更新最后部分数据异常",e);
            } finally {
                for (Map.Entry<KpiType, PreparedStatement> en : map.entrySet()) {
                    JdbcUtil.close(conn, en.getValue(), null); //关闭所有能关闭的资源
                }
            }
        }
    }
}