package storm.ubiquitous.spouts;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.Map;

import backtype.storm.coordination.BatchOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BasePartitionedTransactionalSpout;
import backtype.storm.transactional.TransactionAttempt;
import backtype.storm.transactional.partitioned.IPartitionedTransactionalSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;

public class BatchSpoutFile extends BasePartitionedTransactionalSpout<SpoutTransactionMetadataFile> {

    private static final long serialVersionUID = 1L;
    static BufferedReader reader;
    static FileReader fileReader;
    static boolean completed; 
    static int i = 1;
    @Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
    	declarer.declare(new Fields("txid", "words"));		
    }

    @Override
	public Map<String, Object> getComponentConfiguration() {
	// TODO Auto-generated method stub
	return null;
    }

    @Override
	public IPartitionedTransactionalSpout.Coordinator getCoordinator(Map conf, TopologyContext context) {
	// TODO Auto-generated method stub
	return new WordsTransactionalSpoutCoordinator();
    }

    @Override
	public IPartitionedTransactionalSpout.Emitter<SpoutTransactionMetadataFile> getEmitter(Map conf, TopologyContext context) {
	// TODO Auto-generated method stub
	return new WordsTransactionalSpoutEmitter();
    }
	
    public static class WordsTransactionalSpoutCoordinator implements IPartitionedTransactionalSpout.Coordinator {
		
	@Override
	public boolean isReady() {
	    // TODO Auto-generated method stub
	    try {
	    	return true;				
	    } catch (Exception e) {
		System.out.println(e);
		return false;
	    }	
	}

	@Override
	public void close() {
	    try {
	    	reader.close();
	    	fileReader.close();
	    } catch (IOException e) {
	    	e.printStackTrace();
	    }
	}

	@Override
	public int numPartitions() {
	    return 1;
		}
    }

    public static class WordsTransactionalSpoutEmitter implements IPartitionedTransactionalSpout.Emitter<SpoutTransactionMetadataFile> {
	@Override
	    public void close() {
	    try {
	    	reader.close();
	    	fileReader.close();
	    } catch (IOException e) {
	    	e.printStackTrace();
	    }
	}
	
	@Override
	    public SpoutTransactionMetadataFile emitPartitionBatchNew(TransactionAttempt tx, BatchOutputCollector collector,int partition, SpoutTransactionMetadataFile lastPartitionMeta) {
	    SpoutTransactionMetadataFile metadata = null;
	    
	   if(completed)
	    	return null;
	    
	    try {
	    	System.out.println("In emitBatchNew() and Emitting from Partition: "+ partition);
	    	if(lastPartitionMeta != null)
	    		i=lastPartitionMeta.fileName+1;
	    	System.out.println("Emitting Batchno " + i);
	    	readAndEmit(i, tx, collector);	
	    	metadata = new SpoutTransactionMetadataFile(i,false);	    		    	
	    } 
	    catch (Exception e) {
	    	System.out.println(e);
	    }
	    return metadata;				
	}

	@Override
	    public void emitPartitionBatch(TransactionAttempt tx, BatchOutputCollector collector, int partition,
					   				   SpoutTransactionMetadataFile partitionMeta) {
		//Replay the failed batch only.    
		if(partitionMeta.fileName == i)
		    {
		    	System.out.println("Replaying BatchNo: " + partitionMeta.fileName);
		    	readAndEmit(partitionMeta.fileName , tx, collector);
		    }
		}
	
	public void readAndEmit(int fileName, TransactionAttempt tx, BatchOutputCollector collector) {
			String str;
			try{
				fileReader = new FileReader("src/main/resources/"+fileName+".txt");
		    	reader = new BufferedReader(fileReader);
		    	System.out.println("Reading from file : "+fileName);
		    	while((str = reader.readLine())!=null) {
					collector.emit(new Values(tx,str));
				}
			} catch (Exception e) {
				System.out.println("Exception: "+e);
				completed = true;
			}			
    	}
   }    
}