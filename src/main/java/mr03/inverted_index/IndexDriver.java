package mr03.inverted_index;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;

public class IndexDriver {

    public static void main(String[] args) throws Exception {
        String input = "/Users/moustafa.alaa/moustafa_ws/hadoop-map-reduce/InputData/inverted_index/input/";
        String output = "/Users/moustafa.alaa/moustafa_ws/hadoop-map-reduce/InputData/inverted_index/output";
        System.setProperty("HADOOP_USER_NAME", "root");
        Configuration conf = new Configuration();
        //conf.set("fs.defaultFS", "hdfs://192.168.56.202:9000");
        FileSystem fs = FileSystem.get(conf);
        boolean exists = fs.exists(new Path(output));
        if(exists) {
            fs.delete(new Path(output), true);
        }
        Job job = Job.getInstance(conf);
        job.setJarByClass(IndexDriver.class);

        job.setMapperClass(IndexMapper.class);
        job.setCombinerClass(IndexCombiner.class);
        job.setReducerClass(IndexReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job, new Path(input));
        FileOutputFormat.setOutputPath(job, new Path(output));
        System.exit(job.waitForCompletion(true)?0:1);

    }

}
