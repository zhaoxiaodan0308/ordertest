package com.phone.analystic.hive;

import com.phone.analystic.modle.base.PaymentTypeDimension;
import com.phone.analystic.mr.service.IDimension;
import com.phone.analystic.mr.service.impl.IDimensionImpl;
import com.phone.common.GlobalConstants;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.log4j.Logger;

/**
 * FileName: PaymentTypeDimensionUdf
 * Author: zhao
 * Date: 2018/10/4 20:32
 * Description:支付手段查找id
 * History:
 * <author> <time> <version> <desc>
 * 作者姓名 修改时间 版本号 描述
 */
public class PaymentTypeDimensionUdf extends UDF {

    private static final Logger logger = Logger.getLogger(PaymentTypeDimensionUdf.class);
    private static IDimension iDimension = new IDimensionImpl();

    public static int evaluate(String paymentType) {

        if (StringUtils.isEmpty(paymentType)) {
            paymentType = GlobalConstants.DEFAULT_VALUE;
        }

        int id = -1;

        PaymentTypeDimension paymentTypeDimension = null;
        try {
            paymentTypeDimension = new PaymentTypeDimension(paymentType);
            id = iDimension.getDiemnsionIdByObject(paymentTypeDimension);
        } catch (Exception e) {
            logger.error("获取支付手段id失败", e);
        }
        return id;
    }


//    public static void main(String[] args) {
//        System.out.println(PaymentTypeDimensionUdf.evaluate("支付宝"));
//    }
}
