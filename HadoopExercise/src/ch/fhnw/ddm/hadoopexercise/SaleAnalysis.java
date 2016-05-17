package ch.fhnw.ddm.hadoopexercise;

import java.io.IOException;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class SaleAnalysis extends Configured implements Tool {
	
	final static String inputFile = "res//purchases.txt";
	final static String outputFile = "res//saleanalysis_output";
	
	public static void main(String[] args) throws Exception {
	    int res = ToolRunner.run(new SaleAnalysis(), args);
	    System.exit(res);
	}
	
	@Override
	public int run(String[] args) throws Exception {	
		Job job = Job.getInstance(getConf(), "minimapred");
		
		job.setJarByClass(this.getClass());
		
		job.setMapperClass(SaleAnalysisMapper.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(DoubleWritable.class);
		
		job.setReducerClass(SaleAnalysisReducer.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(DoubleWritable.class);		
		
	    FileInputFormat.addInputPath(job, new Path(inputFile));
	    FileOutputFormat.setOutputPath(job, new Path(outputFile));
	    
	    return job.waitForCompletion(true) ? 0 : 1;
	}
	
	public static class SaleAnalysisMapper extends Mapper<LongWritable, Text, Text, DoubleWritable> {
		final static int indexCategory = 3;
		final static int indexAmount = 4;
		
		private Text category = new Text();
		private DoubleWritable amount = new DoubleWritable();
		
		@Override
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			String line = value.toString();			
			String[] values = line.split("\\t");
			
			if (values.length > indexCategory && values.length > indexAmount) {				
				try {
					category.set(values[indexCategory]);
					amount.set(Double.parseDouble(values[indexAmount]));	
					
					context.write(category, amount);
				} catch (NumberFormatException e) {
					// don't write
				}	
			}
		}
	}

	public static class SaleAnalysisReducer extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {
		private DoubleWritable sum = new DoubleWritable();
		
		@Override
		public void reduce(Text key, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException {
			double total = 0;
			
			for (DoubleWritable val : values) {
				total += val.get();
			}
			
			sum.set(total);
			
			context.write(key, sum);
		}
	}
}