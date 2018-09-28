package com.phone.hive;

import com.phone.Util.TimeUtil;
import com.phone.analystic.modle.base.DateDimension;
import com.phone.analystic.mr.service.IDimension;
import com.phone.analystic.mr.service.impl.IDimensionImpl;
import com.phone.common.DateEnum;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.log4j.Logger;

/**
 * FileName: DateDimensionUdf
 * Author: zhao
 * Date: 2018/9/28 17:53
 * Description:获取时间维度id
 * History:
 * <author> <time> <version> <desc>
 * 作者姓名 修改时间 版本号 描述
 */
public class DateDimensionUdf extends UDF {

    private static final Logger logger = Logger.getLogger(DateDimensionUdf.class);
    private IDimension iDimension = new IDimensionImpl();

    public int evaluate(String date) {
        if (StringUtils.isEmpty(date)) {
            date = TimeUtil.getYesterday();
        }

        DateDimension dateDimension = null;

        int id = -1;
        try {
            dateDimension = DateDimension.buildDate(TimeUtil.parseString2Long(date), DateEnum.DAY);

            id = iDimension.getDiemnsionIdByObject(dateDimension);
        } catch (Exception e) {
            logger.error("获取dateDimession失败", e);
        }

        return id;
    }

}
