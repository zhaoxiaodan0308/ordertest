package com.phone.hive;

import com.phone.analystic.modle.base.EventDimension;
import com.phone.analystic.mr.service.IDimension;
import com.phone.analystic.mr.service.impl.IDimensionImpl;
import com.phone.common.GlobalConstants;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.log4j.Logger;

/**
 * FileName: EventDimensionUdf
 * Author: zhao
 * Date: 2018/9/28 19:06
 * Description:获取事件维度id
 * History:
 * <author> <time> <version> <desc>
 * 作者姓名 修改时间 版本号 描述
 */
public class EventDimensionUdf extends UDF {

    private static final Logger logger = Logger.getLogger(EventDimensionUdf.class);

    private IDimension iDimension = new IDimensionImpl();

    public int evaluate(String category, String action) {

        if (StringUtils.isEmpty(category)) {
            category = GlobalConstants.DEFAULT_VALUE;
        }

        if (StringUtils.isEmpty(action)) {
            category = GlobalConstants.DEFAULT_VALUE;
        }
        int id = -1;
        try {
            EventDimension eventDimension = new EventDimension(category, action);
            id = iDimension.getDiemnsionIdByObject(eventDimension);
        } catch (Exception e) {
            logger.error("获取eventDimension失败", e);
        }
        return id;
    }
}
