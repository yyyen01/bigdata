package lesson.bigdata;


import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;


public class UsageSorter {
    
    
    //mapper2 : Read the output of Mapper1 and Reduce1 and use the UsageBean as key for sorting
    public static class SortUsageMapper extends Mapper<LongWritable, Text, UsageBean, NullWritable> {
        @Override
        protected void map(LongWritable key, Text value, Mapper<LongWritable, Text,  UsageBean, NullWritable>.Context context)
                throws IOException, InterruptedException {
                    String[] fields = value.toString().split("\\t",-1);
    
                    String phone = fields[0];
                    long upStream = Long.parseLong(fields[1]);
                    long downStream = Long.parseLong(fields[2]);
                    
                   context.write(new UsageBean(phone, upStream, downStream),NullWritable.get());
    
        }
    }

     //Reducer2: To do sorting according to the sum of usage
     public static class SortUsageReducer extends Reducer<UsageBean,NullWritable,Text, UsageBean> {
        @Override
        protected void reduce(UsageBean key, Iterable<NullWritable> values, Reducer< UsageBean, NullWritable, Text, UsageBean>.Context context)
                throws IOException, InterruptedException {

                                       
                String phone = key.getPhoneNum();
                context.write(new Text(phone), key);
        }
        }

}
