package storm.ubiquitous.spouts;

import java.io.Serializable;

public class SpoutTransactionMetadataFile implements Serializable{
	private static final long serialVersionUID = 1L;
	int fileName;
	boolean completed;
    public SpoutTransactionMetadataFile(int fileName,boolean completed)	{
    	this.fileName = fileName;
    	this.completed = completed;
	}	
}
