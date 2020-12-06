package tm;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;


/**
 * 统计每一个商家被光顾的记录 reducer
 */
public class MerchantTotalReducer extends Reducer<Text, IntWritable, Text, Text> {


    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        //对相同商家的被光顾累加即可
        int count = 0;
        for (IntWritable v : values) {
            count += v.get();
        }
        context.write(new Text("商家id:"+key.toString()), new Text("商家出货量:"+count));

    }

}