package lesson.bigdata;

import java.io.IOException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class UsageReducer extends Reducer<Text,UsageBean,Text, UsageBean> {
    @Override
    protected void reduce(Text key, Iterable<UsageBean> values, Reducer< Text, UsageBean, Text, UsageBean>.Context context)
            throws IOException, InterruptedException {
            long upStreamCounter = 0;
            long downStreamCounter = 0;
    
            for (UsageBean usageBean : values) {
                upStreamCounter += usageBean.getUpStream();
                downStreamCounter += usageBean.getDownStream();
            }
    
            context.write(key,new UsageBean(key.toString(), upStreamCounter, downStreamCounter));
    }
    }
