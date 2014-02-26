package storm.ubiquitous.spouts;

import java.io.Serializable;

public class SpoutTransactionMetadataFile implements Serializable{
	private static final long serialVersionUID = 1L;
	boolean completed;
	int fileName;
    public SpoutTransactionMetadataFile(int fileName,boolean completed)	{
    	this.fileName = fileName;
    	this.completed = completed;
	}	
}
