package ch.fhnw.ddm.hadoopexercise.saleanalysis;

import java.io.File;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * Main Klasse, welche ein ToolRunner initialisiert und ausführt 
 * 
 * @author Denis Augsburger, Tobias Giess, Ralf Jeppesen
 */
public class SaleAnalysis2 extends Configured implements Tool {
	
	final static String inputFile = "res//purchases.txt";
	final static String outputPath = "res//saleanalysis_output";
	
	/**
	 * Main Einstiegsfunktion
	 * 
	 * @param args Not used
	 */
	public static void main(String[] args) throws Exception {
	    int res = ToolRunner.run(new SaleAnalysis2(), args);
	    System.exit(res);
	}
	
	/**
	 * Hadoop Tool Runner, welcher ein Job konfiguriert und ausführt 
	 * 
	 * @param args Not used
	 */
	@Override
	public int run(String[] args) throws Exception {
		// Instanz, aufgrund der bestehende Konfiguration, laden
		Job job = Job.getInstance(getConf(), "mapreducesaleanalysis");
		
		// Ausführendes Jar, aufgrund der Klasse (diese Klasse), setzen 
		job.setJarByClass(this.getClass());
		
		// Eigene Mapper and Reducer Klasse setzen
		job.setMapperClass(SaleAnalysisMapper.class);
		job.setReducerClass(SaleAnalysisReducer2.class);

		// OutputFormat, von Key und Value, der Map-/Reduce-Funktion setzen
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(DoubleWritable.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);	
		
		// OutputFormater-Klasse der Reduce-Funktion setzen, welche den Output formatiert
		// job.setOutputFormatClass(SaleAnalysisFileOutputFormat.class); 
		
		// Output Ordner löschen
		FileUtils.deleteDirectory(new File(outputPath)); 

		// Input File / Output Path angeben 
	    FileInputFormat.addInputPath(job, new Path(inputFile));
	    FileOutputFormat.setOutputPath(job, new Path(outputPath));
	    
	    // Job ausführen und Resultat abwarten
	    return job.waitForCompletion(true) ? 0 : 1;
	}
}