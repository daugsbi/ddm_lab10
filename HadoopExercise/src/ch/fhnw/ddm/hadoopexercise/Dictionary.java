package ch.fhnw.ddm.hadoopexercise;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.partition.HashPartitioner;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class Dictionary extends Configured implements Tool {
	
	final static String[] inputFiles = { "res//French.txt", "res//German.txt", "res//Italian.txt" };
	final static String outputFile = "res//dictionary_output";
	
	public static void main(String[] args) throws Exception {
	    int res = ToolRunner.run(new Dictionary(), args);
	    System.exit(res);
	}
	
	@Override
	public int run(String[] args) throws Exception {
		Configuration conf = getConf();
		conf.set("mapreduce.input.keyvaluelinerecordreader.key.value.separator", "\t");

		Job job = Job.getInstance(conf);
		
		job.setJarByClass(this.getClass());
		
		job.setInputFormatClass(KeyValueTextInputFormat.class);
		job.setMapperClass(DictionaryMapper.class);
		
		job.setReducerClass(DictionaryReducer.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		for (String file : inputFiles) {
			FileInputFormat.addInputPath(job, new Path(file));	
		}

		FileOutputFormat.setOutputPath(job, new Path(outputFile));
		
	    return job.waitForCompletion(true) ? 0 : 1;
	}
	
	public static class DictionaryMapper extends Mapper<Text, Text, Text, Text> {
		private Text translation = new Text();
		
		@Override
		public void map(Text key, Text value, Context context) throws IOException, InterruptedException {
			String line = value.toString();
			
			for (String word : line.split(",")) {
				translation.set(word.trim());
				
				context.write(key, translation);				
			}
		}
	}

	public static class DictionaryReducer extends Reducer<Text, Text, Text, Text> {
		private Text translations = new Text();
		
		@Override
		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
	        StringBuilder sb = new StringBuilder();
			
			for (Text val : values){
				if (sb.length() > 0) {
					sb.append("|");
				}
				
				sb.append(val.toString());
	        }
			
			translations.set(sb.toString());
			
			context.write(key, translations);
		}
	}
}