import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.util.StringUtils;

import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.conf.Configuration;

import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.fs.Path;

public class UsageCalculator {

    private static Job job1;

    public static class UsageBean implements WritableComparable<UsageBean> {
        private String phoneNum;
        private long upStream;
        private long downStream;
        private long sumUsage;
    
        public String getPhoneNum() {
            return this.phoneNum;
        }
    
        public void setPhoneNum(String phoneNum) {
            this.phoneNum = phoneNum;
        }
    
        public long getUpStream() {
            return this.upStream;
        }
    
        public void setUpStream(long upStream) {
            this.upStream = upStream;
        }
    
        public long getDownStream() {
            return this.downStream;
        }
    
        public void setDownStream(long downStream) {
            this.downStream = downStream;
        }
    
        public long getSumUsage() {
            return this.sumUsage;
        }
    
        public void setSumUsage(long sumUsage) {
            this.sumUsage = sumUsage;
        }
    
        public UsageBean(){}
    
        public UsageBean(String phoneNum,long upStream,long downStream) {
            super();
            this.phoneNum = phoneNum;
            this.upStream = upStream;
            this.downStream = downStream;
            this.sumUsage = upStream + downStream;
        }
    
        public void readFields(DataInput input) throws IOException{
            phoneNum = input.readUTF();
            upStream = input.readLong();
            downStream = input.readLong();
            sumUsage = input.readLong();
        }
    
        public void write(DataOutput output) throws IOException{
            output.writeUTF(phoneNum);
            output.writeLong(upStream);
            output.writeLong(downStream);
            output.writeLong(sumUsage);
        }
        
    
        @Override
        public String toString() {
            return ""+upStream+"\t"+downStream+"\t"+sumUsage;
        }
    
       @Override
       public int compareTo(UsageBean o) {
          return sumUsage > o.sumUsage ? -1 : 1;
       }
        
    }

   //Mapper1 - read original input file and parse
    public static class UsageMapper extends Mapper<LongWritable, Text, Text, UsageBean> {
        @Override
        protected void map(LongWritable key, Text value, Mapper<LongWritable, Text,  Text, UsageBean>.Context context)
                throws IOException, InterruptedException {
                    String[] fields = value.toString().split("\\t",-1);
    
                    String phone = fields[1];
                    long upStream = Long.parseLong(fields[8]);
                    long downStream = Long.parseLong(fields[9]);
                    
                   context.write(new Text(phone), new UsageBean(phone, upStream, downStream));
    
        }
    }

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

    //Reducer1 : to aggregate all usage data
    public static class UsageReducer extends Reducer<Text,UsageBean,Text, UsageBean> {
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

        //Reducer2: To do sorting according to the sum of usage
        public static class SortUsageReducer extends Reducer<UsageBean,NullWritable,Text, UsageBean> {
            @Override
            protected void reduce(UsageBean key, Iterable<NullWritable> values, Reducer< UsageBean, NullWritable, Text, UsageBean>.Context context)
                    throws IOException, InterruptedException {

                                           
                    String phone = key.getPhoneNum();
                    context.write(new Text(phone), key);
            }
            }

        public static void main(String[] args) throws Exception {
            Configuration conf = new Configuration();
            //job1 : to aggregate all usage data
            job1 = Job.getInstance(conf,"Hp Usage Calculator");
    
            job1.setJarByClass(UsageCalculator.class);
            job1.setMapperClass(UsageMapper.class);
            job1.setReducerClass(UsageReducer.class);
             job1.setMapOutputKeyClass(Text.class);
             job1.setMapOutputValueClass(UsageBean.class);            
             job1.setOutputKeyClass(UsageBean.class);
             job1.setOutputValueClass(NullWritable.class);
             FileInputFormat.setInputPaths(job1, new Path(args[0]));
             FileOutputFormat.setOutputPath(job1, new Path(args[1])); //output for job1
             job1.waitForCompletion(true);


             Configuration conf2 = new Configuration();
             //job2 : to sort according to the total usage data
             Job job2 = Job.getInstance(conf2,"Hp Usage Sorter");
     
             job2.setJarByClass(UsageCalculator.class);
             job2.setMapperClass(SortUsageMapper.class);
             job2.setReducerClass(SortUsageReducer.class);
             job2.setMapOutputKeyClass(UsageBean.class);
             job2.setMapOutputValueClass(NullWritable.class);            
             job2.setOutputKeyClass(Text.class);
             job2.setOutputValueClass(UsageBean.class);
              FileInputFormat.setInputPaths(job2, new Path(args[1]));  //output for job1
              FileOutputFormat.setOutputPath(job2, new Path(args[2]));
             System.exit(job2.waitForCompletion(true) ? 0 : 1);
    
        }
    
}
