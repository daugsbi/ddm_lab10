package ch.fhnw.ddm.hadoopexercise.dictionary;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * Mapper, welcher aus den Inputdaten die Wörter (Englisch) und die Übersetzungen aufsplittet und rausschreibt
 * 
 * @author Denis Augsburger, Tobias Giess, Ralf Jeppesen
 */
public class DictionaryMapper extends Mapper<Text, Text, Text, Text> {
	private Text translation = new Text();
	
	/**
	 * Die eigentliche Map Funktion, welche die Wörter (Englisch) und die Übersetzungen aufsplittet und rausschreibt
	 * 
	 * @param key Wort
	 * @param values Übersetzungen, getrennt
	 * @param context Context in welcher der Map-Output geschrieben wird
	 * @output Pro Zeile im File ein Eintrag im Context mit [Key = Englisches Wort; Value = Übersetzung]
	 */
	@Override
	public void map(Text key, Text value, Context context) throws IOException, InterruptedException {
		String line = value.toString();
		
		// Splittet die Übersetzungen, welche mit einem "," getrennt sind, auf und schreibt sie in den Context
		for (String word : line.split(",")) {
			translation.set(word.trim());
			
			context.write(key, translation);				
		}
	}
}
