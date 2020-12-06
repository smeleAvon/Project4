package tm;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Comparator;
import java.util.TreeSet;


/**
 * 统计双⼗⼀最热⻔的商品 reducer
 */
public class HotItemReducer extends Reducer<Text, IntWritable, Text, Text> {

    class Tuple2 {
        Text item;
        IntWritable value;

        public Tuple2(Text item, IntWritable value) {
            this.item = item;
            this.value = value;
        }
    }

    //存放top100的数据
    protected TreeSet<Tuple2> top100Set = new TreeSet<Tuple2>(new Comparator<Tuple2>() {

        public int compare(Tuple2 o1, Tuple2 o2) {
            if (o1.value.get() >= o2.value.get()) {
                return -1;
            } else {
                return 1;
            }
        }
    });


    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        //对相同商品的数据累加即可
        int count = 0;
        for (IntWritable v : values) {
            count += v.get();
        }


        Tuple2 wordCount = new Tuple2(new Text(key.toString()), new IntWritable(count));
        if (top100Set.size() < 100) {
            //when top10Set size < 10 ,add it directly
            top100Set.add(wordCount);
        } else {
            // /when top10Set size >= 10,poll the smallest item and add the new WordNeighborWritable
            if (top100Set.last().value.get() < count) {
                top100Set.pollLast();
                top100Set.add(wordCount);
            }
        }
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        int index = 1;
        while (!top100Set.isEmpty()) {
            Tuple2 tuple2 = top100Set.pollFirst();//output  desc
            context.write(new Text("第"+index+"名的商品id:"+tuple2.item.toString()),new Text("购买量:"+tuple2.value.get()) );
            index++;
        }

    }
}