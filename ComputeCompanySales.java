import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class ComputeCompanySales {

public static class MapperClass extends
  Mapper<LongWritable, Text, Text, IntWritable> {
 public void map(Longwritable key, Text value, Context context) 
			throws IOException, InterruptedException {
		
		Text companyName = null;
		companyName.set(new String("")) ;

		String[] values = value.toString().split("\\|");
		
		boolean companyNameNull = values[0].equalsIgnoreCase("NA");
		boolean productNameNull = values[1].equalsIgnoreCase("NA");
		if(companyNameNull && productNameNull {
			context.write(companyName.set(values[0]),new IntWritable(1));
		}
		
	}
}

public static class ReducerClass extends
  Reducer<Text, IntWritable, Text, IntWritable> {
 public void reduce(Text key, Iterable<IntWritable> valueList,
   Context con) throws IOException, InterruptedException {
  try {
   Float total = (float) 0;
   int count = 0;
   for (IntWritable var : valueList) {
    count++;
   }
   con.write(key, new IntWritable(count));
  } catch (Exception e) {
   e.printStackTrace();
  }
 }
}

public static void main(String[] args) {
 Configuration conf = new Configuration();
 try {
  Job job = Job.getInstance(conf, "ComputeCompanySales");
  job.setJarByClass(ComputeCompanySales.class);
  job.setMapperClass(MapperClass.class);
  job.setReducerClass(ReducerClass.class);
  job.setOutputKeyClass(Text.class);
  job.setOutputValueClass(IntWritable.class);
  FileInputFormat.addInputPath(job, new Path(args[0]));
  FileOutputFormat.setOutputPath(job, new Path(args[1]));
  System.exit(job.waitForCompletion(true) ? 0 : 1);
 } catch (IOException e) {
  e.printStackTrace();
 } catch (ClassNotFoundException e) {
  e.printStackTrace();
 } catch (InterruptedException e) {
  e.printStackTrace();
 }

}
}