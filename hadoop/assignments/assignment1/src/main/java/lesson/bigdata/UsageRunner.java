package lesson.bigdata;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.conf.Configuration;

import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.fs.Path;

public class UsageRunner {
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        //job1 : to aggregate all usage data
        Job job1 = Job.getInstance(conf,"Hp Usage Calculator");

        job1.setJarByClass(UsageRunner.class);
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
 
         job2.setJarByClass(UsageRunner.class);
         job2.setMapperClass(UsageSorter.SortUsageMapper.class);
         job2.setReducerClass(UsageSorter.SortUsageReducer.class);
         job2.setMapOutputKeyClass(UsageBean.class);
         job2.setMapOutputValueClass(NullWritable.class);            
         job2.setOutputKeyClass(Text.class);
         job2.setOutputValueClass(UsageBean.class);
          FileInputFormat.setInputPaths(job2, new Path(args[1]));  //output for job1
          FileOutputFormat.setOutputPath(job2, new Path(args[2]));
         System.exit(job2.waitForCompletion(true) ? 0 : 1);

    }
}
