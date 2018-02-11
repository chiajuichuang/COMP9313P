package comp9313.lab5;

import java.util.ArrayList;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.ArrayWritable;

public class StringArrayWritable extends ArrayWritable {
	
	public StringArrayWritable() {
		super(Text.class);
		set(new Text[]{});
	}

	public StringArrayWritable(ArrayList<String> array) {
		this();
		int n = array.size();
		Text [] elements = new Text[n];
		for(int i=0;i<n;i++){
			elements[i] = new Text(array.get(i));
		}
		set(elements);
	}
	
	public StringArrayWritable(Text[] array) {
		this();
		set(array);
	}
	
	public String[] toStrings(){
		Writable [] array = get();
		int n = array.length;
		
		String[] elements = new String[n];
		for(int i=0;i<n;i++){
			elements[i] = ((Text)array[i]).toString();
		}
		return elements;
	}
	
	//serialize the array, separate strings by ","
	public String toString(){
		return StringUtils.join(toStrings(), " ");
	}
}
