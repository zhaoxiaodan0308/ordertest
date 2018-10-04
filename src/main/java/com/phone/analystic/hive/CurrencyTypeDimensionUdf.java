package com.phone.analystic.hive;

import com.phone.analystic.modle.base.CurrencyTypeDimension;
import com.phone.analystic.mr.service.IDimension;
import com.phone.analystic.mr.service.impl.IDimensionImpl;
import com.phone.common.GlobalConstants;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.log4j.Logger;

/**
 * FileName: CurrencyTypeDimensionUdf
 * Author: zhao
 * Date: 2018/10/4 20:32
 * Description:支付货币类型查找id
 * History:
 * <author> <time> <version> <desc>
 * 作者姓名 修改时间 版本号 描述
 */
public class CurrencyTypeDimensionUdf extends UDF {

    private static final Logger logger = Logger.getLogger(CurrencyTypeDimensionUdf.class);
    private static IDimension iDimension = new IDimensionImpl();

    public static int evaluate(String currencyType) {

        if (StringUtils.isEmpty(currencyType)) {
            currencyType = GlobalConstants.DEFAULT_VALUE;
        }

        int id = -1;

        CurrencyTypeDimension currencyTypeDimension = null;
        try {
            currencyTypeDimension = new CurrencyTypeDimension(currencyType);
            id = iDimension.getDiemnsionIdByObject(currencyTypeDimension);
        } catch (Exception e) {
            logger.error("获取支付货币id失败", e);
        }
        return id;
    }


//    public static void main(String[] args) {
//        System.out.println(CurrencyTypeDimensionUdf.evaluate("CNY"));
//    }
}
