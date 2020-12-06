package tm;


import org.apache.hadoop.io.Text;

import java.io.IOException;

/**
 * 统计统计最受年轻⼈(age < 30)关注的商家 reducer
 */
public class MerchantGirlLoveReducer extends HotItemReducer {
    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        int index = 1;
        while (!top100Set.isEmpty()) {
            Tuple2 tuple2 = top100Set.pollFirst();//output  desc
            context.write(new Text("第"+index+"名的商家id:"+tuple2.item.toString()), new Text("出货量:"+tuple2.value.get()));
            index++;
        }

    }
}