package com.phone.common;

import org.apache.commons.lang.StringUtils;

/**
 * FileName: DateEnum
 * Author: zhao
 * Date: 2018/9/20 17:03
 * Description:日期的枚举
 * History:
 * <author> <time> <version> <desc>
 * 作者姓名 修改时间 版本号 描述
 */
public enum DateEnum {
    YEAR("year"),
    SEASON("season"),
    MONTH("month"),
    WEEK("week"),
    DAY("day"),
    HOUR("hour");

    public String dateType;

    DateEnum(String dateType) {
        this.dateType = dateType;
    }

    /**
     * 根据type返回对应的枚举
     * @param type
     * @return
     */
    public static DateEnum  valueOfDateType(String type){
        if(StringUtils.isNotEmpty(type)){
            for(DateEnum enums:values()){
                if(enums.dateType.equals(type)){
                    return enums;
                }
            }
        }
        return null;
    }
}
