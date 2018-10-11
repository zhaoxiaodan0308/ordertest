package com.phone.analystic.modle;

import com.phone.analystic.modle.base.BaseDimension;
import com.phone.analystic.modle.base.LocationDimension;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * FileName: StatsUserDimension
 * Author: zhao
 * Date: 2018/9/20 19:16
 * Description: 可以用于用户模块和浏览器模块的map和reduce阶段输出的key的类型
 * History:
 * <author> <time> <version> <desc>
 * 作者姓名 修改时间 版本号 描述
 */
public class StatsLocationDimension extends StatsBaseDimension {
    private StatsCommonDimension statsCommonDimension = new StatsCommonDimension();
    private LocationDimension locationDimension = new LocationDimension();

    public StatsLocationDimension() {
    }

    public StatsLocationDimension(StatsCommonDimension statsCommonDimension, LocationDimension locationDimension) {
        this.statsCommonDimension = statsCommonDimension;
        this.locationDimension = locationDimension;
    }

    @Override
    public int compareTo(BaseDimension o) {
        StatsLocationDimension other = (StatsLocationDimension) o;
        int temp = statsCommonDimension.compareTo(other.getStatsCommonDimension());
        if (temp != 0) {
            return temp;
        }
        return locationDimension.compareTo(other.getLocationDimension());
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        this.statsCommonDimension.write(dataOutput);
        this.locationDimension.write(dataOutput);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        this.statsCommonDimension.readFields(dataInput);
        this.locationDimension.readFields(dataInput);
    }

    public StatsCommonDimension getStatsCommonDimension() {
        return statsCommonDimension;
    }

    public void setStatsCommonDimension(StatsCommonDimension statsCommonDimension) {
        this.statsCommonDimension = statsCommonDimension;
    }

    public LocationDimension getLocationDimension() {
        return locationDimension;
    }

    public void setLocationDimension(LocationDimension locationDimension) {
        this.locationDimension = locationDimension;
    }

    @Override
    public String toString() {
        return "StatsLocationDimension{" +
                "statsCommonDimension=" + statsCommonDimension +
                ", locationDimension=" + locationDimension +
                '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        StatsLocationDimension that = (StatsLocationDimension) o;

        if (statsCommonDimension != null ? !statsCommonDimension.equals(that.statsCommonDimension) : that.statsCommonDimension != null)
            return false;
        return locationDimension != null ? locationDimension.equals(that.locationDimension) : that.locationDimension == null;
    }

    @Override
    public int hashCode() {
        int result = statsCommonDimension != null ? statsCommonDimension.hashCode() : 0;
        result = 31 * result + (locationDimension != null ? locationDimension.hashCode() : 0);
        return result;
    }
}
