package com.phone.hive;

import com.phone.analystic.modle.base.PlatformDimension;
import com.phone.analystic.mr.service.IDimension;
import com.phone.analystic.mr.service.impl.IDimensionImpl;
import com.phone.common.GlobalConstants;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.sql.SQLException;

/**
 * FileName: PlatformDimensionUdf
 * Author: zhao
 * Date: 2018/9/28 19:13
 * Description:获取平台维度id
 * History:
 * <author> <time> <version> <desc>
 * 作者姓名 修改时间 版本号 描述
 */
public class PlatformDimensionUdf extends UDF {

    private static final Logger logger = Logger.getLogger(PlatformDimensionUdf.class);
    IDimension iDimension = new IDimensionImpl();

    public int evaluate(String platform) {

        int id = -1;
        try {
            PlatformDimension platformDimension = PlatformDimension.getInstance(platform);

            id = iDimension.getDiemnsionIdByObject(platformDimension);
        } catch (Exception e) {
            logger.error("获取platformDimension失败", e);
        }
        return id;
    }
}
