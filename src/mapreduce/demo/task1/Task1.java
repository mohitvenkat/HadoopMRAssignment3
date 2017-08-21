package mapreduce.demo.task1;

import org.apache.hadoop.fs.Path; 
import org.apache.hadoop.conf.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat; 
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat; 
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat; 
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;


import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;

public class Task1 {
	@SuppressWarnings("deprecation")
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		Job job = new Job(conf, "DemoTask1");
		job.setJarByClass(Task1.class);

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);

		//job.setOutputKeyClass(IntWritable.class);
		//job.setOutputValueClass(IntWritable.class);
		job.setMapperClass(Task1Mapper.class);
	//	job.setReducerClass(Task1Reducer.class);
		 
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		FileInputFormat.addInputPath(job, new Path(args[0])); 
		FileOutputFormat.setOutputPath(job,new Path(args[1]));
	

	    job.setOutputKeyClass(Text.class);

	    job.setOutputValueClass(Text.class);

	    FileInputFormat.addInputPath(job, new Path(args[0]));

	    FileOutputFormat.setOutputPath(job, new Path(args[1]));

	    System.exit(job.waitForCompletion(true) ? 0 : 1);
		job.waitForCompletion(true);
	}
}
