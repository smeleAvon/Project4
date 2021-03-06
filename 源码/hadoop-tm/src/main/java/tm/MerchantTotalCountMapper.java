package tm;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;


/**
 * 统计每一个商家被光顾的记录 mapper
 */
public class MerchantTotalCountMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString().trim();
        if (line.length() <= 0) {
            return;
        }
        String[] word_array = line.split(",");
        if (word_array.length != 7 || !word_array[5].equals("1111")) {
            //过滤不正常的数据
            return;
        }
        String action_type = word_array[6];
        //过滤点击类型的数据
        if (action_type.equals("0")) {
            return;
        }
        String merchant_id = word_array[3];
        //统计商品id 和数量即可
        context.write(new Text(merchant_id), new IntWritable(1));

    }

}