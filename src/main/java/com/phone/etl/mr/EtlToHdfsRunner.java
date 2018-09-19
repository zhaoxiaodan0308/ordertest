package com.phone.etl.mr;

import com.phone.common.GlobalConstants;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;

import java.io.IOException;


/**
 * FileName: EtlToHdfsRunner
 * Author: zhao
 * Date: 2018/9/19 19:52
 * Description:
 * History:
 * <author> <time> <version> <desc>
 * 作者姓名 修改时间 版本号 描述
 */
public class EtlToHdfsRunner implements Tool {
    private static Logger logger = Logger.getLogger(EtlToHdfsRunner.class);
    Configuration conf = new Configuration();

    @Override
    public int run(String[] args) throws Exception {
        Configuration conf = this.getConf();
        //设置输入参数的时间
        this.handleArgs(conf, args);

        Job job = Job.getInstance(conf, "etl to hdfs");

        //设置运行的类
        job.setJarByClass(EtlToHdfsRunner.class);

        //设置map和输出
        job.setMapperClass(EtlToHdfs.class);
        job.setMapOutputKeyClass(LogWritable.class);
        job.setMapOutputValueClass(NullWritable.class);

        //没有reduce
        job.setNumReduceTasks(0);

        //输入输出文件设置
        handleInputOutput(job);

        return job.waitForCompletion(true) ? 1 : 0;
    }

    @Override
    public void setConf(Configuration configuration) {
        configuration = this.conf;
    }

    @Override
    public Configuration getConf() {
        return conf;
    }

    //解析获取的时间
    private void handleArgs(Configuration conf, String[] args) {
        String date = null;
        if (args.length > 0) {
            for (int i = 0; i < args.length; i++) {
                if (args[i].equals("-d")) {
                    if (i + 1 <= args.length - 1) {
                        date = args[i + 1];
                        break;
                    }
                }
            }
            if (StringUtils.isEmpty(date)) {
                throw new RuntimeException("获取时间参数异常");
            } else {
                conf.set(GlobalConstants.RUNNING_DATE, date);
            }
        }
    }


    //设置输入输出文件
    private void handleInputOutput(Job job) {
        Configuration conf = job.getConfiguration();
        String[] strings = conf.get(GlobalConstants.RUNNING_DATE).split("-");
        String m = strings[1];
        String d = strings[2];
        try {
            FileSystem fs = FileSystem.get(conf);
            Path inpath = new Path("/log/" + m + "/" + d);
            Path oupath = new Path("/ods/" + m + "/" + d);

            if (fs.exists(inpath)) {
                FileInputFormat.setInputPaths(job, inpath);
            } else {
                throw new RuntimeException("输入路径不存储在.inpath:" + inpath.toString());
            }

            if (fs.exists(oupath)) {
                fs.delete(oupath, true);
            }

            FileOutputFormat.setOutputPath(job, oupath);

        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static void main(String[] args) {
        try {
            ToolRunner.run(new Configuration(),new EtlToHdfsRunner(),args);
        } catch (Exception e) {
            logger.error("运行异常");
        }
    }
}
