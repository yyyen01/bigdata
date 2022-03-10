package lesson.bigdata;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class UsageMapper extends Mapper<LongWritable, Text, Text, lesson.bigdata.UsageBean> {
    @Override
    protected void map(LongWritable key, Text value, Mapper<LongWritable, Text,  Text, lesson.bigdata.UsageBean>.Context context)
            throws IOException, InterruptedException {
                String[] fields = value.toString().split("\\t",-1);

                String phone = fields[1];
                long upStream = Long.parseLong(fields[8]);
                long downStream = Long.parseLong(fields[9]);
                
               context.write(new Text(phone), new lesson.bigdata.UsageBean(phone, upStream, downStream));

    }
}
