package storm.ubiquitous.spouts;

import java.io.Serializable;

public class SpoutTransactionMetadata implements Serializable{
	private static final long serialVersionUID = 1L;
	boolean completed;
	String str;
	public SpoutTransactionMetadata(String str, boolean completed)	{
		this.str = str;
		this.completed = completed;
	}	
}
