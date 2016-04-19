/**
 * MapReduce job that pipes input to output as MapReduce-created key-val pairs
 */

import java.io.IOException;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * Trivial MapReduce job that pipes input to output as MapReduce-created key-value pairs.
 */
public class Advertising extends Configured implements Tool {

    public static void main(String[] args) throws Exception {
       int res = ToolRunner.run(new Configuration(), new Advertising(), args);
       System.exit(res);
    }
    
    @Override
    public int run(String[] args) throws Exception {

       if (args.length < 3) {
          System.err.println("Error: Wrong number of parameters");
          System.err.println("Expected: [in] [out]");
	  System.exit(1);
       }
		  
       Configuration conf = getConf();
		  
       Job job = new Job(conf, "Our entire project");
       job.setJarByClass(Advertising.class);
		  
       job.setMapperClass(Advertising.IdentityMapper.class);
       job.setReducerClass(Advertising.IdentityReducer.class);
		  
       FileInputFormat.addInputPath(job, new Path(args[0]));
       FileInputFormat.addInputPath(job, new Path(args[1]));
       FileOutputFormat.setOutputPath(job, new Path(args[2]));
		  
       return job.waitForCompletion(true) ? 0 : 1;
    }
    
    /**
     * map: (LongWritable, Text) --> (LongWritable, Text)
     * NOTE: Keys must implement WritableComparable, values must implement Writable
     */
    public static class IdentityMapper extends Mapper<LongWritable, Text, LongWritable, Text> {
      
       @Override
	   public void map(LongWritable key, Text val, Context context)
          throws IOException, InterruptedException {
	   System.out.println("key: "+key);
	   System.out.println("val: "+val);
          context.write(key, val);
       } 
    }
    
    /**
     * reduce: (LongWritable, Text) --> (LongWritable, Text)
     */
    public static class IdentityReducer extends Reducer <LongWritable, Text, LongWritable, Text> {
      
       @Override
       public void reduce(LongWritable key, Iterable<Text> values, Context context) 
	  throws IOException, InterruptedException {
	  // write (key, val) for every value
	  //output = 0;
	  for (Text val : values) {
	      //add +1 to output
	      context.write(key, val);
	  }
       }
    }
}