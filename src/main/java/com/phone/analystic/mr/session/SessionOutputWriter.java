package com.phone.analystic.mr.session;

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
 * Description:将信息写入mysql
 * History:
 * <author> <time> <version> <desc>
 * 作者姓名 修改时间 版本号 描述
 */
public class SessionOutputWriter implements IOutputWritter {
    private static Logger logger = Logger.getLogger(SessionOutputWriter.class);

    @Override
    public void ouput(Configuration conf, StatsBaseDimension key, StatsOutpuValue value, PreparedStatement ps, IDimension iDimension) {


        try {
            StatsUserDimension k = (StatsUserDimension) key;
            OutputWritable v = (OutputWritable) value;
            int i = 0;

            //新增会员赋值
            int sessionID = ((IntWritable) (v.getValue().get(new IntWritable(-1)))).get();
            int sessionlength =((IntWritable) (v.getValue().get(new IntWritable(-2)))).get();

                    ps.setInt(++i, iDimension.getDiemnsionIdByObject(k.getStatsCommonDimension().getDateDimension()));
            ps.setInt(++i, iDimension.getDiemnsionIdByObject(k.getStatsCommonDimension().getPlatformDimension()));
            if (value.getKpi().equals(KpiType.BROWSER_SESSION)) {
                ps.setInt(++i, iDimension.getDiemnsionIdByObject(k.getBrowserDimension()));
            }
            ps.setInt(++i, sessionID);
            ps.setInt(++i, sessionlength);
            ps.setString(++i, conf.get(GlobalConstants.RUNNING_DATE));
            ps.setInt(++i, sessionID);
            ps.setInt(++i, sessionlength);

            ps.addBatch(); //添加到批处理中


        } catch (Exception e) {
            logger.error("新增会员指标ps赋值错误", e);
        }

    }
}
