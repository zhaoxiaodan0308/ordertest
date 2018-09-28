package com.phone.Util;

import com.phone.common.GlobalConstants;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * FileName: MemberUtil
 * Author: zhao
 * Date: 2018/9/26 19:15
 * Description:
 * History:
 * <author> <time> <version> <desc>
 * 作者姓名 修改时间 版本号 描述
 */
public class MemberUtil {
    //缓存
    private static Map<String, Boolean> cache = new LinkedHashMap<String, Boolean>() {
        @Override
        protected boolean removeEldestEntry(Map.Entry<String, Boolean> eldest) {
            return this.size() > 1000;
        }
    };


    /**
     * 检测会员ID是否合法
     *
     * @param memeberid
     * @return true是合法，false不合法
     */
    public static boolean checkMemberId(String memeberid) {
        String regex = "^[0-9a-zA-Z].*$";
        if (StringUtils.isNotEmpty(memeberid)) {
            return memeberid.trim().matches(regex);
        }
        return false;
    }

    //查询数据库该id是否存在

    /**
     * 是否是一个新增的会员
     *
     * @param memberId
     * @param conn
     * @param conf
     * @return true是新增会员，false不是新增会员
     */

    public static boolean isNewMember(String memberId, Connection conn, Configuration conf) {

        PreparedStatement ps = null;
        ResultSet rs = null;
        Boolean res = false;  //默认查找不到，为新用户

        try {
            res = cache.get(memberId);
            if (res == null) {
                ps = conn.prepareStatement(conf.get("other_member_info"));
                ps.setString(1, memberId);
                rs = ps.executeQuery();

                if (rs.next()) {
                    res = false;
                }else{
                    res = true;
                }

                cache.put(memberId, res);
            }

        } catch (SQLException e) {
            e.printStackTrace();
        }
        return res == null ? false : res.booleanValue();
    }

    /**
     * 删除莫一天的会员，意在重新跑某一天的新增会员
     * @param conf
     */
    public static void deleteByDay(Configuration conf){
        Connection conn = null;
        PreparedStatement ps = null;
        try {
            conn=JdbcUtil.getConn();
            String created = conf.get(GlobalConstants.RUNNING_DATE);
            ps = conn.prepareStatement(conf.get("other_delete_member_info"));
            ps.setString(1,created);
            ps.execute();
        } catch (SQLException e) {
            e.printStackTrace();
        } finally {
            JdbcUtil.close(conn,ps,null);
        }

    }
}
