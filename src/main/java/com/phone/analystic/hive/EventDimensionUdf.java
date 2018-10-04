package com.phone.analystic.hive;

import com.phone.analystic.modle.base.EventDimension;
import com.phone.analystic.mr.service.IDimension;
import com.phone.analystic.mr.service.impl.IDimensionImpl;
import com.phone.common.GlobalConstants;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hive.ql.exec.UDF;

/**
 * @ClassName EventDimensionUdf
 * @Author lyd
 * @Date $ $
 * @Vesion 1.0
 * @Description 获取事件维度的Id
 **/
public class EventDimensionUdf extends UDF {

    IDimension iDimension = new IDimensionImpl();

    /**
     *
     * @param category
     * @param action
     * @return  事件维度的id
     */
    public int evaluate(String category,String action){
        if(StringUtils.isEmpty(category)){
            category = action = GlobalConstants.DEFAULT_VALUE;
        }
        if(StringUtils.isEmpty(action)){
            action = GlobalConstants.DEFAULT_VALUE;
        }
        int id = -1;

        try {
            EventDimension ed = new EventDimension(category,action);
            id = iDimension.getDiemnsionIdByObject(ed);
        } catch (Exception e) {
           e.printStackTrace();
        }
        return id;
    }


}