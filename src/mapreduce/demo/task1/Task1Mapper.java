package mapreduce.demo.task1;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.*; 

public class Task1Mapper extends Mapper<Longwritable , Text, Text, Text> {
	@SuppressWarnings("null")
	public void map(Longwritable key, Text value, Context context) 
			throws IOException, InterruptedException {
		
		Text emptyText = null;
		emptyText.set(new String("")) ;

		String[] values = value.toString().split("\\|");
		
		boolean companyNameNull = values[0].equalsIgnoreCase("NA");
		boolean productNameNull = values[1].equalsIgnoreCase("NA");
		if(companyNameNull || productNameNull){
			emptyText.set(emptyText.toString() + value);
			context.write(emptyText,new Text());
		}
		
	}
}
