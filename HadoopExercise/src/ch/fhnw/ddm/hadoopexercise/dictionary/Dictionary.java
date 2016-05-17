package ch.fhnw.ddm.hadoopexercise.dictionary;

import java.io.File;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * Main Klasse, welche ein ToolRunner initialisiert und ausführt 
 * 
 * @author Denis Augsburger, Tobias Giess, Ralf Jeppesen
 */
public class Dictionary extends Configured implements Tool {
	
	final static String[] inputFiles = { "res//French.txt", "res//German.txt", "res//Italian.txt" };
	final static String outputPath = "res//dictionary_output";
	
	/**
	 * Main Einstiegsfunktion
	 * 
	 * @param args Not used
	 */
	public static void main(String[] args) throws Exception {
	    int res = ToolRunner.run(new Dictionary(), args);
	    System.exit(res);
	}
	
	/**
	 * Hadoop Tool Runner, welcher ein Job konfiguriert und ausführt 
	 * 
	 * @param args Not used
	 */
	@Override
	public int run(String[] args) throws Exception {
		// Bestehende Konfiguration laden
		Configuration conf = getConf();
		
		// Konfiguration so anpassen, dass als Delimter [Tab] verwendet wird
		conf.set("mapreduce.input.keyvaluelinerecordreader.key.value.separator", "\t");

		// Instanz laden
		Job job = Job.getInstance(conf, "mapreducedictionary");
		
		// Ausführendes Jar, aufgrund der Klasse (diese Klasse), setzen 
		job.setJarByClass(this.getClass());
		
		// InputFormater-Klasse der Map-Funktion setzen, welche den Output formatiert
		job.setInputFormatClass(KeyValueTextInputFormat.class);
		
		// Eigene Mapper and Reducer Klasse setzen
		job.setMapperClass(DictionaryMapper.class);
		job.setReducerClass(DictionaryReducer.class);
		
		// OutputFormat, von Key und Value, der Reduce-Funktion setzen
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		// Input Files angeben 
		for (String file : inputFiles) {
			FileInputFormat.addInputPath(job, new Path(file));	
		}

		// Output Ordner löschen
		FileUtils.deleteDirectory(new File(outputPath)); 

		// Output Path angeben 
		FileOutputFormat.setOutputPath(job, new Path(outputPath));
		
	    // Job ausführen und Resultat abwarten
	    return job.waitForCompletion(true) ? 0 : 1;
	}
}