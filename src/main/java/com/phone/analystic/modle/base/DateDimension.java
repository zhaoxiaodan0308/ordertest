package com.phone.analystic.modle.base;

import com.phone.Util.TimeUtil;
import com.phone.common.DateEnum;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Calendar;
import java.util.Date;
import java.util.Objects;

/**
 * FileName: DateDimension
 * Author: zhao
 * Date: 2018/9/20 10:06
 * Description:日期维度(使用时间戳)
 * History:
 * <author> <time> <version> <desc>
 * 作者姓名 修改时间 版本号 描述
 */
public class DateDimension extends BaseDimension {
    private int id;
    private int year;
    private int season;
    private int month;
    private int week;
    private int day;
    private String type;  //指标类型   天指标
    private Date calendar = new Date(); //日期

    public DateDimension(){

    }
    public DateDimension(int year, int season, int month, int week, int day) {
        this.year = year;
        this.season = season;
        this.month = month;
        this.week = week;
        this.day = day;
    }

    public DateDimension(int year, int season, int month, int week, int day,String type) {
        this(year,season,month,week,day);
        this.type = type;
    }

    public DateDimension(int year, int season, int month, int week, int day,String type,Date calendar) {
        this(year,season,month,week,day,type);
        this.calendar = calendar;
    }

    public DateDimension(int id,int year, int season, int month, int week, int day,String type,Date calendar) {
        this(year,season,month,week,day,type,calendar);
        this.id = id;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeInt(this.id);
        dataOutput.writeInt(this.year);
        dataOutput.writeInt(this.season);
        dataOutput.writeInt(this.month);
        dataOutput.writeInt(this.week);
        dataOutput.writeInt(this.day);
        dataOutput.writeUTF(this.type);
        dataOutput.writeLong(this.calendar.getTime()); //date写成long
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        this.id = dataInput.readInt();
        this.year = dataInput.readInt();
        this.season = dataInput.readInt();
        this.month = dataInput.readInt();
        this.week = dataInput.readInt();
        this.day = dataInput.readInt();
        this.type = dataInput.readUTF();
        this.calendar.setTime(dataInput.readLong());
    }

    /**
     * 根据时间戳和指标类型来获取对应的时间维度对象
     * @param time
     * @param type
     * @return
     */
    public static DateDimension buildDate(long time,DateEnum type){
        Calendar calendar = Calendar.getInstance();
        calendar.clear();
        //获取年份
        int year = TimeUtil.getDateInfo(time,DateEnum.YEAR);
        //获取年度指标的dateDiemension对象.
        if(type.equals(DateEnum.YEAR)){
            calendar.set(year,0,1);//年指标，截止日期为当年的1月1号
            return new DateDimension(year,0,0,0,1,type.dateType,calendar.getTime());
        }
        int season = TimeUtil.getDateInfo(time,DateEnum.SEASON);
        if(type.equals(DateEnum.SEASON)){
            int month = (season*3 - 2);
            calendar.set(year,month-1,1);//截止当前季度的第一个月的一号
            return new DateDimension(year,season,month,0,1,type.dateType,calendar.getTime());
        }

        int month =  TimeUtil.getDateInfo(time,DateEnum.MONTH);
        if(type.equals(DateEnum.MONTH)){
            calendar.set(year,month-1,1);//截止当月1号
            return new DateDimension(year,season,month,0,1,type.dateType,calendar.getTime());
        }
        int week =  TimeUtil.getDateInfo(time,DateEnum.WEEK);
        if(type.equals(DateEnum.WEEK)){
            long firstDayOfWeek = TimeUtil.getFirstDayOfWeek(time);
            year = TimeUtil.getDateInfo(firstDayOfWeek,DateEnum.YEAR);
            season = TimeUtil.getDateInfo(firstDayOfWeek,DateEnum.SEASON);
            month = TimeUtil.getDateInfo(firstDayOfWeek,DateEnum.MONTH);
            return new DateDimension(year,season,month,week,0,type.dateType,new Date(firstDayOfWeek));
        }
        int day =  TimeUtil.getDateInfo(time,DateEnum.DAY);
        if(type.equals(DateEnum.DAY)){
            calendar.set(year,month-1,day);
            return new DateDimension(year,season,month,week,day,type.dateType,calendar.getTime());
        }
        throw  new RuntimeException("暂不支持该时间枚举类型的时间维度获取.type:"+type.dateType);
    }


    @Override
    public int compareTo(BaseDimension o) {
        if(o == this){
            return 0;
        }
        DateDimension other = (DateDimension) o;
        int tmp = this.id - other.id;
        if(tmp != 0){
            return  tmp;
        }
        tmp = this.year - other.year;
        if(tmp != 0){
            return  tmp;
        }
        tmp = this.season - other.season;
        if(tmp != 0){
            return  tmp;
        }
        tmp = this.month - other.month;
        if(tmp != 0){
            return  tmp;
        }
        tmp = this.week - other.week;
        if(tmp != 0){
            return  tmp;
        }
        tmp = this.day - other.day;
        if(tmp != 0){
            return  tmp;
        }
        return this.type.compareTo(other.type);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        DateDimension that = (DateDimension) o;
        return id == that.id &&
                year == that.year &&
                season == that.season &&
                month == that.month &&
                week == that.week &&
                day == that.day &&
                Objects.equals(type, that.type) &&
                Objects.equals(calendar, that.calendar);
    }

    @Override
    public int hashCode() {
        return Objects.hash(id, year, season, month, week, day, type, calendar);
    }

    public int getId() {
        return id;
    }

    public void setId(int id) {
        this.id = id;
    }

    public int getYear() {
        return year;
    }

    public void setYear(int year) {
        this.year = year;
    }

    public int getSeason() {
        return season;
    }

    public void setSeason(int season) {
        this.season = season;
    }

    public int getMonth() {
        return month;
    }

    public void setMonth(int month) {
        this.month = month;
    }

    public int getWeek() {
        return week;
    }

    public void setWeek(int week) {
        this.week = week;
    }

    public int getDay() {
        return day;
    }

    public void setDay(int day) {
        this.day = day;
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public Date getCalendar() {
        return calendar;
    }

    public void setCalendar(Date calendar) {
        this.calendar = calendar;
    }
}