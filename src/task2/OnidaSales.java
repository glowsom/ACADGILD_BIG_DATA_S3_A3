package task2;

import org.apache.hadoop.conf.*;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class OnidaSales {

	@SuppressWarnings("deprecation")
	public static void main(String[] args) throws Exception {
		/*
		 *  Entry point of program which will call TvSalesMapper class
		 *  and every other class needed for the job to be done.
		 */
		Configuration conf = new Configuration();	//Get configuration details
		Job job =new Job(conf, "TvSales");	//Job object is created
		job.setJarByClass(OnidaSales.class);	//Set main class

		job.setMapOutputKeyClass(Text.class);//Represents State name
		job.setMapOutputValueClass(IntWritable.class);//Represents each instance of sale
		
		job.setMapperClass(OnidaSalesMapper.class);	//Set mapper class
		job.setReducerClass(OnidaSalesReducer.class);	//Set reducer class
		
		//Because there isn't a reducer, the framework will handle it
		job.setOutputKeyClass(Text.class);//Represents State names
		job.setOutputValueClass(IntWritable.class);// Represents number of sale instances per state for Onida.
		
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		/*
		 * Input and Output Paths will be provided in command line.
		 */
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		job.waitForCompletion(true);
	}

}
