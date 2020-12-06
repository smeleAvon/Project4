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
 * 计算统计最受年轻⼈(age < 30)关注的商家 作业调度逻辑
 */
public class MerchantGrilLoveApplication {

    public static void main(String[] args) throws Exception {
//        String inputPath = args[0];
//        String outputPath = args[1];
        String inputPath = "D:\\data\\data_format1\\data_format1\\user_log_format1.csv";
        String outputPath = "D:\\data\\data_format1\\data_format1\\love_out";
        Configuration configuration = new Configuration();
        Job job = Job.getInstance(configuration);
        job.addCacheFile(new URI("file:///D:/data/data_format1/data_format1/user_info_format1.csv"));
        FileInputFormat.addInputPath(job, new Path(inputPath));//输入目录
        FileOutputFormat.setOutputPath(job, new Path(outputPath));//输出目录

        job.setJarByClass(MerchantGrilLoveApplication.class);
        job.setOutputKeyClass(Text.class);//reduce后输出到目录中的key的格式
        job.setOutputValueClass(Text.class);//reduce后输出到目录中的value的格式
        job.setMapOutputKeyClass(Text.class);//reduce后输出到目录中的key的格式
        job.setMapOutputValueClass(IntWritable.class);//reduce后输出到目录中的value的格式

        job.setMapperClass(MerchantGirlLoveMapper.class);
        //job.setCombinerClass(MerchantGirlLoveReducer.class);
        job.setReducerClass(MerchantGirlLoveReducer.class);
        job.setNumReduceTasks(1);
        job.waitForCompletion(true);
    }

}