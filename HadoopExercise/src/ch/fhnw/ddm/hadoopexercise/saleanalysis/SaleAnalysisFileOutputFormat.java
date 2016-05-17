package ch.fhnw.ddm.hadoopexercise.saleanalysis;

import java.io.IOException;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * FileOutputFormat welche ein SaleAnalysisRecordWriter erstellt
 *  
 * @author Denis Augsburger, Tobias Giess, Ralf Jeppesen
 * @source http://johnnyprogrammer.blogspot.ch/2012/01/custom­file­output­in­hadoop.html 
 */
public class SaleAnalysisFileOutputFormat extends FileOutputFormat<Text, DoubleWritable> { 
    
	/**
	 * Gibt einen neuen SaleAnalysisRecordWriter zurück
	 * 
	 * @param context Context, anhand welchem der Datenpfad für den DataOutputStream verwendet wird
	 */
	@Override 
	public RecordWriter<Text, DoubleWritable> getRecordWriter(TaskAttemptContext context) throws IOException, InterruptedException { 
    	Path path = FileOutputFormat.getOutputPath(context);
	    Path fullPath = new Path(path, "result.txt"); 
	    
	    FileSystem fs = path.getFileSystem(context.getConfiguration()); 
	    FSDataOutputStream fileOut = fs.create(fullPath, context); 
	 
	    return new SaleAnalysisRecordWriter(fileOut); 
    } 
} 