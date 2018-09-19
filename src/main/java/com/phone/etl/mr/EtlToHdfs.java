package com.phone.etl.mr;

import com.phone.etl.ip.LogUtil;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.Map;

/**
 * @ClassName EtlToHdfs
 * @Author lyd
 * @Date $ $
 * @Vesion 1.0
 * @Description 原数据：/log/09/18
 * 原数据：/log/09/19
 * 清洗后的存储目录: /ods/09/18
 * 清洗后的存储目录: /ods/09/19
 **/
public class EtlToHdfs {
    static class MyMapper extends Mapper<LongWritable, Text, LogUtil.UserInfo, NullWritable>{
        Map<String, LogUtil.UserInfo> infoMap=null;

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String s = value.toString();
            infoMap = LogUtil.parserLog(s);
            for(Map.Entry<String, LogUtil.UserInfo> map: infoMap.entrySet()){
                context.write(map.getValue(),NullWritable.get());
            }
        }
    }

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        //1.配置集群的参数
        Configuration conf = new Configuration();
        //配置环境
        conf.set("fs.defaultFS","hdfs://hadoop01:9000");

        //2.获取作业实例
        Job job = Job.getInstance(conf, "EtlToHdfs");

        //3.获取本业务的job路径
        job.setJarByClass(EtlToHdfs.class);

        //4.获取本业务的map类
        job.setMapperClass(MyMapper.class);
        //获取本作业输出
        job.setOutputKeyClass(LogUtil.UserInfo.class);
        job.setOutputValueClass(NullWritable.class);

        //6.设置作业要处理的数据源
        FileInputFormat.setInputPaths(job,new Path("/log/09/19"));

        //7.设置作业要处理的输出路径
        FileOutputFormat.setOutputPath(job,new Path("hdfs://hadoop01:9000/ods/09/19"));

        //8.提交作业

        boolean b = job.waitForCompletion(true);

        System.exit(b ? 0 : 1);
    }
}