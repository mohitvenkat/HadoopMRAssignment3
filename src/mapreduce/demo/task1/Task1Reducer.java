package mapreduce.demo.task1;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;

public class Task1Reducer extends Reducer<IntWritable, IntWritable, IntWritable, IntWritable>
{	
	public void reduce(IntWritable key, Iterable<IntWritable> values,Context context) throws IOException, InterruptedException
	{
		int maxVal = Integer.MIN_VALUE;
		for (IntWritable value : values) {
			if (value.get() > maxVal) {
				maxVal = value.get();
			}
		}

		context.write(key, new IntWritable(maxVal));
	}
}
