package mapreduce.demo.task1;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.*; 

public class Task1Mapper extends Mapper<Longwritable , Text, Longwritable, Text> {
	@SuppressWarnings("null")
	public void map(Longwritable key, Text value, Context context) 
			throws IOException, InterruptedException {
		String[] lineArray = value.toString().split("\n");
		
		Text emptyText = null;
		emptyText.set(new String("")) ;

		for(String line: lineArray){
		boolean companyNameNull = line.split("\\|")[0].equalsIgnoreCase("NA");
		boolean productNameNull = line.split("\\|")[1].equalsIgnoreCase("NA");
		if(companyNameNull || productNameNull){
			emptyText.set(emptyText.toString() + line);
		}
		}
		context.write(key, emptyText);
	}
}
