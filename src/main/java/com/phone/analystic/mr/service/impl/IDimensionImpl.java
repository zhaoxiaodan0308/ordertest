package com.phone.analystic.mr.service.impl;

import com.phone.Util.JdbcUtil;
import com.phone.analystic.modle.base.*;
import com.phone.analystic.mr.service.IDimension;

import java.io.IOException;
import java.sql.*;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * FileName: IDimensionImpl
 * Author: zhao
 * Date: 2018/9/20 20:43
 * Description:获取基础维度id的实现
 * History:
 * <author> <time> <version> <desc>
 * 作者姓名 修改时间 版本号 描述
 */
public class IDimensionImpl implements IDimension {

    //定义内存缓存：维度信息-维度id
    private Map<String, Integer> cache = new LinkedHashMap<String, Integer>() {
        @Override
        protected boolean removeEldestEntry(Map.Entry<String, Integer> eldest) {
            return this.size() > 5000;
        }
    };

    /**
     * 1、根据维度对象里面的属性值，赋值给对应的sql，然后查询，如果有则返回对应的维度Id.
     * 如果查询没有，则先添加到数据库中并返回对应的id值。
     *
     * @param dimension 基础维度的对象
     * @return
     * @throws IOException
     * @throws SQLException
     */
    @Override
    public int getDiemnsionIdByObject(BaseDimension dimension) throws IOException, SQLException {

        Connection conn = null;
        try {
            //构造cachekey，查询缓存中是否存在
            String cacheKey = buildCacheKey(dimension);
            if (cache.containsKey(cacheKey)) {
                return cache.get(cacheKey);
            }

            //走到此处，缓存中不存在该key，需要从数据库中获取
            //构建访问数据库需要的sql
            String[] sqls = null;
            if (dimension instanceof KpiDimension) {
                sqls = buildKpiSqls(dimension);
            } else if (dimension instanceof PlatformDimension) {
                sqls = buildPlatformSqls(dimension);
            } else if (dimension instanceof DateDimension) {
                sqls = buildDateSqls(dimension);
            } else if (dimension instanceof BrowserDimension) {
                sqls = buildBrowserSqls(dimension);
            } else if (dimension instanceof LocationDimension) {
                sqls = buildLocationSqls(dimension);
            } else if (dimension instanceof EventDimension) {
                sqls = buildEventSqls(dimension);
            } else if (dimension instanceof CurrencyTypeDimension) {
                sqls = buildCurrencyTypeSqls(dimension);
            } else if (dimension instanceof PaymentTypeDimension) {
                sqls = buildPaymentTypeSqls(dimension);
            }


            //获取jdbc的连接
            conn = JdbcUtil.getConn();
            int id = -1;
            synchronized (this) {
                id = this.executSql(conn, sqls, dimension);
            }

            //将结果存储到cache中
            this.cache.put(cacheKey, id);
            return id;
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            JdbcUtil.close(conn, null, null);
        }

        throw new RuntimeException("插入基础维度类异常.");
    }


    /**
     * 构建访问数据库需要的sql
     *
     * @param dimension
     * @return
     */
    private String[] buildKpiSqls(BaseDimension dimension) {
        String insertSql = "insert into `dimension_kpi` (kpi_name) values(?)";
        String selectSql = "select id from `dimension_kpi` where kpi_name = ?";
        return new String[]{insertSql, selectSql};
    }

    private String[] buildPlatformSqls(BaseDimension dimension) {
        String insertSql = "insert into `dimension_platform` (platform_name) values(?)";
        String selectSql = "select id from `dimension_platform` where platform_name = ?";
        return new String[]{insertSql, selectSql};
    }

    private String[] buildDateSqls(BaseDimension dimension) {
        String insertSql = "INSERT INTO `dimension_date` (`year`, `season`, `month`, `week`, `day`, `type`, `calendar`) VALUES(?, ?, ?, ?, ?, ?, ?)";
        String selectSql = "SELECT `id` FROM `dimension_date` WHERE `year` = ? AND `season` = ? AND `month` = ? AND `week` = ? AND `day` = ? AND `type` = ? AND `calendar` = ?";
        return new String[]{insertSql, selectSql};
    }

    private String[] buildBrowserSqls(BaseDimension dimension) {
        String insertSql = "INSERT INTO `dimension_browser` (`browser_name`, `browser_version`) VALUES(?,?)";
        String selectSql = "SELECT `id` FROM `dimension_browser` WHERE `browser_name` = ? AND `browser_version` = ?";
        return new String[]{insertSql, selectSql};
    }

    private String[] buildEventSqls(BaseDimension dimension) {
        String insertSql = "INSERT INTO `dimension_event` (`category`, `action`) VALUES(?,?)";
        String selectSql = "SELECT `id` FROM `dimension_event` WHERE `category` = ? AND `action` = ? ";
        return new String[]{insertSql, selectSql};
    }

    private String[] buildLocationSqls(BaseDimension dimension) {
        String insertSql = "INSERT INTO `dimension_location` (`country`, `province`,`city`) VALUES(?,?,?)";
        String selectSql = "SELECT `id` FROM `dimension_location` WHERE `country` = ? AND `province` = ? AND `city` = ?";
        return new String[]{insertSql, selectSql};
    }

    private String[] buildCurrencyTypeSqls(BaseDimension dimension) {
        String insertSql = "INSERT INTO `dimension_currency_type` (`currency_name`) VALUES(?)";
        String selectSql = "SELECT `id` FROM `dimension_currency_type` WHERE `currency_name` = ? ";
        return new String[]{insertSql, selectSql};
    }

    private String[] buildPaymentTypeSqls(BaseDimension dimension) {
        String insertSql = "INSERT INTO `dimension_payment_type` (`payment_type`) VALUES(?)";
        String selectSql = "SELECT `id` FROM `dimension_payment_type` WHERE `payment_type` = ? ";
        return new String[]{insertSql, selectSql};
    }

    /**
     * 构建维度的key
     *
     * @param dimension
     * @return
     */
    private String buildCacheKey(BaseDimension dimension) {
        StringBuffer sb = new StringBuffer();
        if (dimension instanceof BrowserDimension) {
            BrowserDimension browser = (BrowserDimension) dimension;
            sb.append("browser_");
            sb.append(browser.getBrowserName());
            sb.append(browser.getBrowserVersion());
            //browser_IE8.0
        } else if (dimension instanceof DateDimension) {
            DateDimension date = (DateDimension) dimension;
            sb.append("date_");
            sb.append(date.getYear());
            sb.append(date.getSeason());
            sb.append(date.getMonth());
            sb.append(date.getWeek());
            sb.append(date.getDay());
            sb.append(date.getType());
        } else if (dimension instanceof KpiDimension) {
            sb.append("kpi_");
            KpiDimension kpi = (KpiDimension) dimension;
            sb.append(kpi.getKpiName());
            //kpi_new_user
        } else if (dimension instanceof PlatformDimension) {
            sb.append("platform_");
            PlatformDimension platform = (PlatformDimension) dimension;
            sb.append(platform.getPlatformName());
            //new_user
        } else if (dimension instanceof LocationDimension) {
            sb.append("location_");
            LocationDimension location = (LocationDimension) dimension;
            sb.append(location.getCountry());
            sb.append(location.getProvince());
            sb.append(location.getCity());
        } else if (dimension instanceof EventDimension) {
            sb.append("event_");
            EventDimension event = (EventDimension) dimension;
            sb.append(event.getCategory());
            sb.append(event.getAction());
        } else if (dimension instanceof CurrencyTypeDimension) {
            sb.append("currencyType_");
            CurrencyTypeDimension currencyType = (CurrencyTypeDimension) dimension;
            sb.append(currencyType.getCurrency_name());
        } else if (dimension instanceof PaymentTypeDimension) {
            sb.append("paymentType_");
            PaymentTypeDimension paymentType = (PaymentTypeDimension) dimension;
            sb.append(paymentType.getPayment_type());
        }


        return sb.toString();
    }


    /**
     * 执行sql
     *
     * @param conn
     * @param sqls
     * @param dimension
     * @return
     */
    private int executSql(Connection conn, String[] sqls, BaseDimension dimension) {
        PreparedStatement ps = null;
        ResultSet rs = null;

        try {
            //获取执行的sql
            ps = conn.prepareStatement(sqls[1]);
            this.setArgs(dimension, ps); //为查询语句赋值
            rs = ps.executeQuery();
            if (rs.next()) {
                return rs.getInt(1);
            }

            //表中没有该数据
            ps = conn.prepareStatement(sqls[0], Statement.RETURN_GENERATED_KEYS);
            this.setArgs(dimension, ps); //为插入语句赋值
            ps.executeUpdate();
            rs = ps.getGeneratedKeys();
            if (rs.next()) {
                return rs.getInt(1);
            }
        } catch (SQLException e) {
            e.printStackTrace();
        } finally {
            JdbcUtil.close(null, ps, rs);
        }

        return 0;
    }

    private void setArgs(BaseDimension dimension, PreparedStatement ps) {
        try {
            int i = 0;
            if (dimension instanceof KpiDimension) {
                KpiDimension kpi = (KpiDimension) dimension;
                ps.setString(++i, kpi.getKpiName());
            } else if (dimension instanceof DateDimension) {
                DateDimension date = (DateDimension) dimension;
                ps.setInt(++i, date.getYear());
                ps.setInt(++i, date.getSeason());
                ps.setInt(++i, date.getMonth());
                ps.setInt(++i, date.getWeek());
                ps.setInt(++i, date.getDay());
                ps.setString(++i, date.getType());
                ps.setDate(++i, new Date(date.getCalendar().getTime()));
            } else if (dimension instanceof PlatformDimension) {
                PlatformDimension platform = (PlatformDimension) dimension;
                ps.setString(++i, platform.getPlatformName());
            } else if (dimension instanceof BrowserDimension) {
                BrowserDimension browser = (BrowserDimension) dimension;
                ps.setString(++i, browser.getBrowserName());
                ps.setString(++i, browser.getBrowserVersion());
            } else if (dimension instanceof LocationDimension) {
                LocationDimension location = (LocationDimension) dimension;
                ps.setString(++i, location.getCountry());
                ps.setString(++i, location.getProvince());
                ps.setString(++i, location.getCity());
            } else if (dimension instanceof EventDimension) {
                EventDimension event = (EventDimension) dimension;
                ps.setString(++i, event.getCategory());
                ps.setString(++i, event.getAction());
            } else if (dimension instanceof CurrencyTypeDimension) {
                CurrencyTypeDimension currencyType = (CurrencyTypeDimension) dimension;
                ps.setString(++i, currencyType.getCurrency_name());
            }else if (dimension instanceof PaymentTypeDimension) {
                PaymentTypeDimension paymentType = (PaymentTypeDimension) dimension;
                ps.setString(++i, paymentType.getPayment_type());
            }

        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    //自定输出格式OutputFormat
//    DBOutputFormat
}
