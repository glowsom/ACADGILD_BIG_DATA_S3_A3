package task1;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class CompanySalesReducer extends Reducer<Text, IntWritable, Text, IntWritable>
{	
	private int sum;//Track all sale counts
	
	@Override
	public void reduce(Text key, Iterable<IntWritable> values,Context context) throws IOException, InterruptedException
	{
		sum = 0;
		
		//Iterable values contains all instances of sales per company name
		for (IntWritable value : values) {
				sum += value.get(); //Sum up all instances of sales per company name
		}
		//Result will be the company name and it's sales count.
		context.write(key, new IntWritable(sum));
	}
}