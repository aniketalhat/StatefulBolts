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

public class BatchSpout extends BasePartitionedTransactionalSpout<SpoutTransactionMetadata>{

	private static final long serialVersionUID = 1L;
	static BufferedReader reader;
	static FileReader fileReader;
	static boolean completed;
	static String str;
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
	public IPartitionedTransactionalSpout.Coordinator getCoordinator(
			Map conf, TopologyContext context) {
		// TODO Auto-generated method stub
		return new WordsTransactionalSpoutCoordinator();
	}

	@Override
	public IPartitionedTransactionalSpout.Emitter<SpoutTransactionMetadata> getEmitter(
			Map conf, TopologyContext context) {
		// TODO Auto-generated method stub
		return new WordsTransactionalSpoutEmitter();
	}
	
	public static class WordsTransactionalSpoutCoordinator implements IPartitionedTransactionalSpout.Coordinator {
		
		@Override
		public boolean isReady() {
			// TODO Auto-generated method stub
			try {
				fileReader = new FileReader("src/main/resources/words.txt");
				return true;
				
			} catch (Exception e) {
				System.out.println(e);
				return false;
			}
			
		}

		@Override
		public void close() {
			try {
				fileReader.close();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}

		@Override
		public int numPartitions() {
			// TODO Auto-generated method stub
			return 1;
		}		
		
	}

	public static class WordsTransactionalSpoutEmitter implements IPartitionedTransactionalSpout.Emitter<SpoutTransactionMetadata>	{
		@Override
		public void close() {
			try {
				fileReader.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
			
		}

		@Override
		public SpoutTransactionMetadata emitPartitionBatchNew(
				TransactionAttempt tx, BatchOutputCollector collector,
				int partition, SpoutTransactionMetadata lastPartitionMeta) {
			SpoutTransactionMetadata metadata;
			reader = new BufferedReader(fileReader);
				if(completed)
				{
					return null;
				}
				try {
						while((str = reader.readLine())!=null) {
						metadata = new SpoutTransactionMetadata(str, false);
						System.out.println("In emitBatchNew and partition: "+ partition);
						emitPartitionBatch(tx, collector, partition, metadata);
						
						}
				} catch (Exception e) {
						System.out.println(e);
						
				}
				finally{
					completed = true;
					metadata = new SpoutTransactionMetadata(str, completed);					
				}
				
				return metadata;
				
		}

		@Override
		public void emitPartitionBatch(TransactionAttempt tx,
				BatchOutputCollector collector, int partition,
				SpoutTransactionMetadata partitionMeta) {
			if(partitionMeta.completed == true)
				return;
			collector.emit(new Values(tx, str));
			System.out.println("In emitBatch and partition: "+ partition);
		}
		
	}
}
