package ch.fhnw.ddm.hadoopexercise.saleanalysis;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * Mapper, welcher aus den Inputdaten die Kategorie und die Verkaufszahl rausfiltert
 * 
 * @author Denis Augsburger, Tobias Giess, Ralf Jeppesen
 */
public class SaleAnalysisMapper extends Mapper<LongWritable, Text, Text, DoubleWritable> {
	final static int indexCategory = 3;
	final static int indexAmount = 4;
	
	private Text category = new Text();
	private DoubleWritable amount = new DoubleWritable();
	
	/**
	 * Die eigentliche Map Funktion, welche die Kategorie und die Verkaufszahl rausfiltert
	 * 
	 * @param key Key
	 * @param values Eine Zeile im File, getrennt mit Tabs (Format: [date][Tab][time][Tab][store][Tab][category][Tab][amount][Tab][method of payment])
	 * @param context Context in welcher der Map-Output geschrieben wird
	 * @output Pro Wort im File x Einträge im Context mit [Key = Wort; Value = Übersetzung] (wobei x = Anzahl Übersetzungen)
	 */
	@Override
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		String line = value.toString();			
		String[] values = line.split("\t");
		
		// Ist die Zeile gültig?
		if (values.length > indexCategory && values.length > indexAmount) {				
			try {
				category.set(values[indexCategory]);
				amount.set(Double.parseDouble(values[indexAmount]));	
				
				// values[indexAmount] ist ein gültiger Double Wert
				context.write(category, amount);
			} catch (NumberFormatException e) {
				// Kein Output, da values[indexAmount] kein gültiger Double Wert
			}	
		}
	}
}