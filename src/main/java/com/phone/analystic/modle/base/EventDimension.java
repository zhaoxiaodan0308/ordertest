package com.phone.analystic.modle.base;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * FileName: EventDimension
 * Author: zhao
 * Date: 2018/9/28 0:30
 * Description:
 * History:
 * <author> <time> <version> <desc>
 * 作者姓名 修改时间 版本号 描述
 */
public class EventDimension extends BaseDimension {
    private int id;
    private String category;
    private String action;

    public EventDimension() {
    }

    public EventDimension(String category, String action) {
        this.category = category;
        this.action = action;
    }

    public EventDimension(int id, String category, String action) {
        this.id = id;
        this.category = category;
        this.action = action;
    }

    @Override
    public int compareTo(BaseDimension o) {

        if(this == o){
            return 0;
        }

        EventDimension other = (EventDimension) o;

        int temp=this.id - other.id;
        if(temp != 0){
            return  temp;
        }

        temp=this.category.compareTo(other.category);
        if(temp != 0){
            return  temp;
        }

        return this.action.compareTo(other.action);
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeInt(this.id);
        dataOutput.writeUTF(this.category);
        dataOutput.writeUTF(this.action);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        this.id = dataInput.readInt();
        this.category = dataInput.readUTF();
        this.action = dataInput.readUTF();
    }

    public int getId() {
        return id;
    }

    public void setId(int id) {
        this.id = id;
    }

    public String getCategory() {
        return category;
    }

    public void setCategory(String category) {
        this.category = category;
    }

    public String getAction() {
        return action;
    }

    public void setAction(String action) {
        this.action = action;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        EventDimension that = (EventDimension) o;

        if (id != that.id) return false;
        if (category != null ? !category.equals(that.category) : that.category != null) return false;
        return action != null ? action.equals(that.action) : that.action == null;
    }

    @Override
    public int hashCode() {
        int result = id;
        result = 31 * result + (category != null ? category.hashCode() : 0);
        result = 31 * result + (action != null ? action.hashCode() : 0);
        return result;
    }

    @Override
    public String toString() {
        return "EventDimension{" +
                "id=" + id +
                ", category='" + category + '\'' +
                ", action='" + action + '\'' +
                '}';
    }
}
