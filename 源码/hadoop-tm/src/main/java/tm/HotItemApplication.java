package tm;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.net.URI;

/**
 * 计算双⼗⼀最热⻔的商品 作业调度逻辑
 */
public class HotItemApplication {

    public static void main(String[] args) throws Exception {
//        String inputPath = args[0];
//        String outputPath = args[1];
        String inputPath = "D:\\data\\data_format1\\data_format1\\user_log_format1.csv";
        String outputPath = "D:\\data\\data_format1\\data_format1\\hot_out";
        Configuration configuration = new Configuration();
        Job job = Job.getInstance(configuration);
        job.addCacheFile(new URI("file:///D:/data/data_format1/data_format1/user_info_format1.csv"));
        FileInputFormat.addInputPath(job, new Path(inputPath));//输入目录
        FileOutputFormat.setOutputPath(job, new Path(outputPath));//输出目录

        job.setJarByClass(HotItemApplication.class);
        job.setOutputKeyClass(Text.class);//reduce后输出到目录中的key的格式
        job.setOutputValueClass(Text.class);//reduce后输出到目录中的value的格式
        job.setMapOutputKeyClass(Text.class);//reduce后输出到目录中的key的格式
        job.setMapOutputValueClass(IntWritable.class);//reduce后输出到目录中的value的格式

        job.setMapperClass(HotItemMapper.class);
        //job.setCombinerClass(HotItemReducer.class);
        job.setReducerClass(HotItemReducer.class);
        job.setNumReduceTasks(1);
        job.waitForCompletion(true);
    }

}