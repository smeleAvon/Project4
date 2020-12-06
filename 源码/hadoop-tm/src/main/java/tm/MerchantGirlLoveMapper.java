package tm;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.util.HashMap;


/**
 * 统计统计最受年轻⼈(age < 30)关注的商家 mapper
 */
public class MerchantGirlLoveMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

    private HashMap<String, Integer> user_age_map = new HashMap<String,Integer>(4096);

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
        //过滤年龄为30外以上的数据
        String user_id = word_array[0];
        if (!filterAge30(user_id)) {
            return;
        }
        String merchant_id = word_array[3];
        //统计商品id 和数量即可

        context.write(new Text(merchant_id), new IntWritable(1));

    }

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        URI[] files = context.getCacheFiles();
        //读取用户信息数据
        String user_file = files[0].getPath();
        BufferedReader bufferedReader = new BufferedReader(new FileReader(user_file));
        String user = null;
        bufferedReader.readLine();
        while ((user = bufferedReader.readLine()) != null) {
            String[] info = user.split(",");
            if (info.length == 3) {
                String age_range = info[1];
                if (age_range.equals("3") || age_range.equals("2") || age_range.equals("1")) {
                    user_age_map.put(info[0], 1);
                }
            }
        }
    }

    private Boolean filterAge30(String user_id) {
        return user_age_map.containsKey(user_id);
    }

}