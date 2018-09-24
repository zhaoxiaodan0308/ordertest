package com.phone.analystic.mr.service;

import com.phone.analystic.modle.base.BaseDimension;

import java.io.IOException;
import java.sql.SQLException;

/**
 * FileName: IDimension
 * Author: zhao
 * Date: 2018/9/20 20:42
 * Description:根据维度获取对应的id的接口
 * History:
 * <author> <time> <version> <desc>
 * 作者姓名 修改时间 版本号 描述
 */
public interface IDimension {
    int getDiemnsionIdByObject(BaseDimension dimension) throws IOException,SQLException;

}
