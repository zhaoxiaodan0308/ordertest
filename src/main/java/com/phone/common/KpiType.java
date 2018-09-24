package com.phone.common;

/**
 * FileName: KpiType
 * Author: zhao
 * Date: 2018/9/20 19:32
 * Description:
 * History:
 * <author> <time> <version> <desc>
 * 作者姓名 修改时间 版本号 描述
 */
public enum  KpiType {

    NEW_USER("new_user"),
    BROWSER_NEW_USER("browser_new_user");

    public String kpiName;

    KpiType(String kpiName) {
        this.kpiName = kpiName;
    }

    /**
     * 根据kpi的name获取对应的指标
     * @param name
     * @return
     */
    public static KpiType valueOfKpiName(String name){
        for (KpiType k:values()){
            if(k.kpiName.equals(name)){
                return  k;
            }
        }
        return null;
    }
}
