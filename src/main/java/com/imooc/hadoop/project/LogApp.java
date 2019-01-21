package com.imooc.hadoop.project;

import com.kumkee.userAgent.UserAgent;
import com.kumkee.userAgent.UserAgentParser;
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
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * 使用MapReduce来完成我们的需求：统计浏览器的访问次数
 */
public class LogApp {
    /**
     * Map:读取输入的文件
     * Text是java的String
     * LongWritable是java的Long
     * IntWritable是java的Integer
     * 前两个参数是输入，后两个参数是输出
     */
    public static class MyMapper extends Mapper<LongWritable, Text, Text, LongWritable> {

        LongWritable one = new LongWritable(1);
        private UserAgentParser userAgentParser;

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            userAgentParser = new UserAgentParser();
        }

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
            // hadoop类型转java类型：其实就是一行日志信息
            String line = value.toString();


            String source = line.substring(getCharacterPosition(line, "\"", 7)) + 1;
            UserAgent agent = userAgentParser.parse(source);
            String browser = agent.getBrowser();
            // 通过上下文把map的处理结果进行输出
            context.write(new Text(browser),one);

        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            userAgentParser = null;
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
     * 获取指定字符串中指定标识的字符串中出现的索引位置
     */
    private static int getCharacterPosition(String value, String operator, int index) {
        Matcher slashMatcher = Pattern.compile(operator).matcher(value);
        int mIndx = 0;
        while (slashMatcher.find()) {
            mIndx++;
            if (mIndx == index) {
                break;
            }
        }
        return slashMatcher.start();

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
        Job job = Job.getInstance(configuration, "LogApp");

        // 设置job的处理类
        job.setJarByClass(LogApp.class);

        // 设置作业处理的输入路径
        FileInputFormat.setInputPaths(job, new Path(args[0]));

        // 设置map相关参数
        job.setMapperClass(LogApp.MyMapper.class);
        job.setMapOutputKeyClass(Text.class);// 设置输出键的类型:文本数据信息
        job.setMapOutputValueClass(LongWritable.class); // 输出值得类型:值

        //设置reduce相关参数
        job.setReducerClass(LogApp.MyReducer.class);
        job.setOutputKeyClass(Text.class);// 处理结果的键的类型:文本数据信息
        job.setOutputValueClass(LongWritable.class); // 处理结果的值得类型:值

        // 通过job设置Combiner处理类，起始逻辑上和我们的reduce是一样的
        job.setCombinerClass(LogApp.MyReducer.class);

        //设置作业处理的输出路径
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true)?0:1);



    }

}
