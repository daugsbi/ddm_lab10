package ch.fhnw.ddm.HadoopExercise.SaleAnalysis;

import java.io.DataOutputStream;
import java.io.IOException;
import java.text.DecimalFormat;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

/**
 * Der Writer Formatiert einen Double Output in einen schön formatierten Double String
 * 5.7491808439999506E7 -> 57491808.44
 * 
 * @author Denis Augsburger, Tobias Giess, Ralf Jeppesen
 * @source http://johnnyprogrammer.blogspot.ch/2012/01/custom­file­output­in­hadoop.html 
 */
public class SaleAnalysisRecordWriter extends RecordWriter<Text, DoubleWritable> { 		 
	final static DecimalFormat decimalFormat = new DecimalFormat("0.00"); 
	final static String delimiter = "\t";
	private DataOutputStream out; 
	
	/**
	 * Konstruktor
	 * 
	 * @param stream Der DataOutputStream in welchen der Output geschrieben wird 
	 */
	public SaleAnalysisRecordWriter(DataOutputStream stream) { 
		out = stream; 
	} 
	 
	/**
	 * Schliessen des RecordWriter und damit auch des DataOutputStream
	 * 
	 * @param taskAttemptContext Not used
	 */
    @Override 
    public void close(TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException { 
    	out.close(); 
    } 
	 
    /**
     * Schreib Funktion, welche aufgerufen wird und welche genutzt wird um den Output zu formatieren
     * 
     * @param key Kategorie, wird nicht verändert
     * @param value Double Wert, welcher formatiert werden soll
     */
    @Override 
    public void write(Text key, DoubleWritable value) throws IOException, InterruptedException { 
        String formattedAmount = decimalFormat.format(value.get()); 
        
        out.writeBytes(key.toString() + delimiter + formattedAmount + "\r\n"); 
    } 
}