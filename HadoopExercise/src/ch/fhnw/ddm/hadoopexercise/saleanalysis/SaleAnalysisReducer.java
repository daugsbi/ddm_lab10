package ch.fhnw.ddm.hadoopexercise.saleanalysis;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * Reducer, welcher die totalen Verkaufszahlen pro Produktekategorie berechnet
 * 
 * @author Denis Augsburger, Tobias Giess, Ralf Jeppesen
 */
public class SaleAnalysisReducer extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {
	private DoubleWritable sum = new DoubleWritable();
	
	/**
	 * Die eigentliche Reduce Funktion, welche die Summenberechnung durchführt
	 * 
	 * @param key Produktkategorie
	 * @param values Liste aller Verkaufszahlen einer Produktkategorie
	 * @param context Context in welcher der Reduce-Output geschrieben wird
	 * @output Pro Produktkategorie eine Zeile im Format "[Produktkategorie][Tab][Summe Verkaufszahlen]"
	 */
	@Override
	public void reduce(Text key, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException {
		double total = 0;
		
		// Summe berechnen für eine Produktkategorie
		for (DoubleWritable val : values) {
			total += val.get();
		}
		
		sum.set(total);
		
		context.write(key, sum);
	}
}