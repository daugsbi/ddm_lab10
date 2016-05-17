package ch.fhnw.ddm.hadoopexercise.dictionary;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * Reducer, welche die Übersetzungen (Deutsch, Französisch, Italienisch) zu einem Wort (Englisch), zusammenfügt
 * 
 * @author Denis Augsburger, Tobias Giess, Ralf Jeppesen
 */
public class DictionaryReducer extends Reducer<Text, Text, Text, Text> {
	private Text translations = new Text();
	
	/**
	 * Die eigentliche Reduce Funktion, welche die Zusammenfügung durchführt
	 * 
	 * @param key Wort (Englisch)
	 * @param values Übersetzungen in den verschiedenen Sprachen (Deutsch, Französisch, Italienisch)
	 * @param context Context in welcher der Reduce-Output geschrieben wird
	 * @output Pro Wort (Englisch) eine Zeile mit allen Übersetzungen im Format "[Wort][Tab][Übersetzung1]|[Übersetzung2]|..."
	 */
	@Override
	public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        StringBuilder sb = new StringBuilder();
		
        // Fügt die Übersetzungen zusammen
		for (Text val : values){
			if (sb.length() > 0) {
				sb.append("|");
			}
			
			sb.append(val.toString());
        }
		
		translations.set(sb.toString());
		
		// Schreibt die Übersetzungen in den Context raus
		context.write(key, translations);
	}
}