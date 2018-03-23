import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;



public class Task1Driver 
{
    public static void main(String[] args) throws Exception 
    {
        Configuration conf = new Configuration();
       
        conf.set("fs.defaultFS", "hdfs://localhost:54310");
        conf.set("fs.hdfs.impl", org.apache.hadoop.hdfs.DistributedFileSystem.class.getName());
        Job job = Job.getInstance(conf, "Distinct Value");
        job.setJarByClass(Task1Driver.class);
        job.setMapperClass(Task1Mapper.class);
        job.setReducerClass(Task1Reducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        
        FileInputFormat.addInputPath(job, new Path("hdfs://localhost:54310/user/hduser/task1"));
		FileOutputFormat.setOutputPath(job, new Path("hdfs://localhost:54310/user/hduser/task1/output"));

        
        System.exit(job.waitForCompletion(true) ? 0 : 1);
        }
}