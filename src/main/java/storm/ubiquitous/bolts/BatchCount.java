package storm.ubiquitous.bolts;

import java.io.Serializable;
import java.math.BigInteger;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import redis.clients.jedis.Jedis;
import storm.ubiquitous.state.RedisMap;
import backtype.storm.coordination.BatchOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.FailedException;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseTransactionalBolt;
import backtype.storm.transactional.ICommitter;
import backtype.storm.transactional.TransactionAttempt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

public class BatchCount extends BaseTransactionalBolt {
	/**
	 * 
	 * Committer Bolt
	 * @author aniket
	 */
	private static final long serialVersionUID = -2343991642735232104L;
	
	//Attributes of every tuple
	@SuppressWarnings("serial")
	public static class CountValue implements Serializable{
		    public Integer prev_count = null;
		    public int count = 0;
		    public BigInteger txid = null;
		    public long atid = 0L;
		    public long duration; 
	}
	
	public static Map<String, CountValue> INMEMORYDB = new ConcurrentHashMap<String, CountValue>();
	
	TransactionAttempt _id;
	BatchOutputCollector _collector;
	String name;
	Map<String, Integer> counters;
	Integer _count = 1;
	RedisMap mapStore;
	transient Jedis jedis; 
	String redisHost;
	int redisPort;
	static int test;
	
	public BatchCount(String redisHost, int redisPort) {
		this.redisHost = redisHost;
		this.redisPort = redisPort;
	}

	public void prepare(@SuppressWarnings("rawtypes") Map conf, TopologyContext context, 
			    BatchOutputCollector collector, TransactionAttempt id) {
		_id = id;
		this.counters = new HashMap<String, Integer>();
		_collector=collector;
		mapStore=new RedisMap("localhost");
		//jedis = new Jedis(redisHost, redisPort, 1800);
		//jedis.connect();
	}
   
	public void execute(Tuple tuple) {
		name = tuple.getString(1);
		test++;
		if(!counters.containsKey(name)){
		  counters.put(name, _count);
		}
	  
		else{
		  _count = counters.get(name) + 1;
		  counters.put(name, _count);
		}
		//jedis.hincrBy("jazoon:hashtags", name, 1);
		//Raise Exception
		if(test == 50)
			throw new FailedException();
   }    
   
	public void finishBatch(){
		
	   	for (String key : counters.keySet()) {
	    
   		//Create a new instance of every tuple.
		CountValue val = INMEMORYDB.get(key);
		CountValue newVal;

		//Compare for tuple existence.
	       	if (val == null || !val.txid.equals(_id.getTransactionId())) {
	          
	       		//Start Timer.
	       		long starttime = System.nanoTime();
	       		
	    	   	newVal = new CountValue();
	    	   	newVal.txid = _id.getTransactionId();
	    	   	newVal.atid = _id.getAttemptId();
	          
				if (val != null) {
					newVal.prev_count = val.count;
					newVal.count = val.count;
				}

				newVal.count = newVal.count + counters.get(key);
				
				//End Timer.
				long endtime = System.nanoTime();
				
				//Calculate duration.
				long duration = endtime - starttime;
				
				newVal.duration = duration;
				INMEMORYDB.put(key, newVal);						
				
	       	}	
	       	//Tuple already processed.
	       	else {
	         
			newVal = val;
			System.out.println("[ALERT] Re-processing of Tuple: {" +  key + "} with Txid: " + newVal.txid + " and AttemptID: "+ newVal.atid + 
			" avoided, approximate time saved: "+newVal.duration + " (nanosecond)");
	       	}
	        	System.out.println("[OUTPUT] Tuple: {" + key + "} NewCount: " + newVal.count+" PrevCount: "
	        	+ newVal.prev_count + " Txid: "+_id.getTransactionId() + " AttemptID: " + _id.getAttemptId());
	       
	           	_collector.emit(new Values(_id, key, newVal.count, newVal.prev_count));		 
		}
	   	
	       	//Save state of a complete batch.
	   	if(counters.size()>0)
	      	{
	    	  	mapStore.setState(_id.getTransactionId().toByteArray(), INMEMORYDB);
	      	}	   	
   }    
   
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
     		declarer.declare(new Fields("id","name", "count"));
   }
}
 
