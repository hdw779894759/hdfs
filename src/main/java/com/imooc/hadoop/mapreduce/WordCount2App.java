package com.imooc.hadoop.mapreduce;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * 使用MapReduce开发WordCount应用程序
 */
public class WordCount2App {


    /**
     * Map:读取输入的文件
     * Text是java的String
     * LongWritable是java的Long
     * IntWritable是java的Integer
     * 前两个参数是输入，后两个参数是输出
     */
    public static class MyMapper extends Mapper<LongWritable, Text, Text, LongWritable> {

        LongWritable one = new LongWritable(1);

        /**
         *
         * @param key       输入
         * @param value     输入
         * @param context   上下文
         * @throws IOException
         * @throws InterruptedException
         */
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            // hadoop类型转java类型：接收到的每一行数据
            String line = value.toString();

            // 按照指定分隔符【空格】进行拆分
            String[] words = line.split(" ");

            for (String word : words) {
                // 通过上下文把map的处理结果进行输出
                context.write(new Text(word),one);
            }
        }
    }


    /**
     * MyMapper的输出是MyReducer的输入
     * 做归并操作
     */
    public static class MyReducer extends Reducer<Text, LongWritable,Text,LongWritable> {

        /**
         *
         * @param key
         * @param values
         * @param context
         * @throws IOException
         * @throws InterruptedException
         */
        @Override
        protected void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {

            long sum = 0;
            for (LongWritable value : values) {
                // 求key出现的次数总和
                sum += value.get();
            }

            // 最终统计结果输出，以key为键，总和为值
            context.write(key, new LongWritable(sum));

        }
    }


    /**
     * 定义Driver：封装了MapReduce作业的所有信息
     * @param args
     */
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        // 创建Configuration
        Configuration configuration = new Configuration();

        // 准备清理已经存在的输出目录
        Path outPutPath = new Path(args[1]);
        FileSystem fileSystem = FileSystem.get(configuration);
        if (fileSystem.exists(outPutPath)) {
            fileSystem.delete(outPutPath, true);
            System.out.println("output file exists,but is has deleted");
        }
        // 创建Job
        Job job = Job.getInstance(configuration, "wordcount");

        // 设置job的处理类
        job.setJarByClass(WordCount2App.class);

        // 设置作业处理的输入路径
        FileInputFormat.setInputPaths(job, new Path(args[0]));

        // 设置map相关参数
        job.setMapperClass(MyMapper.class);
        job.setMapOutputKeyClass(Text.class);// 设置输出键的类型:文本数据信息
        job.setMapOutputValueClass(LongWritable.class); // 输出值得类型:值

        //设置reduce相关参数
        job.setReducerClass(MyReducer.class);
        job.setOutputKeyClass(Text.class);// 处理结果的键的类型:文本数据信息
        job.setOutputValueClass(LongWritable.class); // 处理结果的值得类型:值

        //设置作业处理的输出路径
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true)?0:1);



    }



}
