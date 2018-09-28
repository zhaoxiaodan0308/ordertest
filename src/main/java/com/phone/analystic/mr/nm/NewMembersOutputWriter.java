package com.phone.analystic.mr.nm;

import com.phone.analystic.modle.StatsBaseDimension;
import com.phone.analystic.modle.StatsUserDimension;
import com.phone.analystic.modle.value.StatsOutpuValue;
import com.phone.analystic.modle.value.reduce.OutputWritable;
import com.phone.analystic.mr.IOutputWritter;
import com.phone.analystic.mr.service.IDimension;
import com.phone.common.GlobalConstants;
import com.phone.common.KpiType;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.log4j.Logger;

import java.sql.Date;
import java.sql.PreparedStatement;

/**
 * FileName: NewUserOutputWriter
 * Author: zhao
 * Date: 2018/9/24 13:26
 * Description:将新用户信息写入mysql
 * History:
 * <author> <time> <version> <desc>
 * 作者姓名 修改时间 版本号 描述
 */
public class NewMembersOutputWriter implements IOutputWritter {
    private static Logger logger = Logger.getLogger(NewMembersOutputWriter.class);

    @Override
    public void ouput(Configuration conf, StatsBaseDimension key, StatsOutpuValue value, PreparedStatement ps, IDimension iDimension) {


        try {
            StatsUserDimension k = (StatsUserDimension) key;
            OutputWritable v = (OutputWritable) value;
            int i = 0;
            switch (value.getKpi()) {
                case NEW_MEMBERS:
                case BROWSER_NEW_MEMBERS:
                    //新增会员赋值
                    int newMembers = ((IntWritable) (v.getValue().get(new IntWritable(-1)))).get();

                    ps.setInt(++i, iDimension.getDiemnsionIdByObject(k.getStatsCommonDimension().getDateDimension()));
                    ps.setInt(++i, iDimension.getDiemnsionIdByObject(k.getStatsCommonDimension().getPlatformDimension()));
                    if (value.getKpi().equals(KpiType.BROWSER_NEW_MEMBERS)) {
                        ps.setInt(++i, iDimension.getDiemnsionIdByObject(k.getBrowserDimension()));
                    }
                    ps.setInt(++i, newMembers);
                    ps.setString(++i, conf.get(GlobalConstants.RUNNING_DATE));
                    ps.setInt(++i, newMembers);

                    ps.addBatch(); //添加到批处理中

                    break;
                case MEMBER_INFO:
                    //member_info表更新
                    String memberID = ((Text) (v.getValue().get(new IntWritable(-2)))).toString();
                    long time = ((LongWritable) (v.getValue().get(new IntWritable(-3)))).get();

                    ps.setString(1, memberID);
                    ps.setDate(2, Date.valueOf(conf.get(GlobalConstants.RUNNING_DATE)));
                    ps.setLong(3, time);
                    ps.setDate(4, Date.valueOf(conf.get(GlobalConstants.RUNNING_DATE)));
                    ps.setDate(5, Date.valueOf(conf.get(GlobalConstants.RUNNING_DATE)));
                    ps.addBatch(); //添加到批处理中
                    break;
                default:
                    break;
            }

        } catch (Exception e) {
            logger.error("新增会员指标ps赋值错误",e);
        }

    }
}
