package storm.ubiquitous.state;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.concurrent.ConcurrentHashMap;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.Response;
import redis.clients.jedis.Transaction;

/**
 * This is an idea to build abstractions for bolts with fault-tolerant state (so if a task dies and gets 
 * reassigned to another machine it still has its state).So generally you keep any persistent state in a 
 * database, oftentimes doing something like waiting to ack() tuples until you've done a batch update to 
 * the database. Stateful bolts will just be a much more efficient way of keeping a large amount of 
 * state at hand in a bolt.
 * @author aniket
 */
public class RedisMap implements IPersistentMap{
	
	String serverURL;
	String className;
	ByteArrayOutputStream byteOut;
	ObjectOutputStream out;
	ByteArrayInputStream byteIn;
	ObjectInputStream in;
	
	public RedisMap(String serverURL) {
		this.serverURL=serverURL;		
	}
	
	public RedisMap(String serverURL, String className)
	{
		this.serverURL=serverURL;
		this.className=className;
	}

	public void setState(byte[] key, Object value) {
		Jedis db = null;
		Kryo kryo = null;
		Transaction multi = null;
		try {
			
			db = new Jedis(serverURL); 
			multi = db.multi();
			
			//Java Serialization
			//byteOut = new ByteArrayOutputStream();
			//out = new ObjectOutputStream(byteOut);
			//out.writeObject(value);
			//out.close();
			//byteOut.close();
						
			kryo = new Kryo();
			Output output = new Output(new ByteArrayOutputStream());
			kryo.writeObject(output, value);
			
			byte [] byteValue=output.toBytes();
			multi.set(key,byteValue);
			multi.save();
			multi.exec();
			output.close();	
			
	      	}
			
		catch(Exception i) {			
			i.printStackTrace();
			
	      	}
					
	}
	public Object getState(byte[] key) {
		Object value = null;
		Jedis db = null;
		Kryo kryo = null;
		Transaction multi= null;
		try {
			 
			 db = new Jedis(serverURL);
			 multi = db.multi();
			 kryo = new Kryo(); 
			 
			 Response<byte[]> store = multi.get(key);
			 multi.exec();
			 
			 //Java De-Serialization
			 //byteIn = new ByteArrayInputStream(store);
			 //in = new ObjectInputStream(byteIn);
			 //value = in.readObject();
			 //in.close();
			 //byteIn.close();
			 
			 Input input = new Input(new ByteArrayInputStream(store.get()));
			 value = kryo.readObject(input, ConcurrentHashMap.class);
			 System.out.println("Bolt state retrieved.");
			 input.close();
		       
	      	}
			
		catch(Exception i) {
			 i.printStackTrace();
			 
	      	}
				
		return value;
		
	}

	@Override
	public void close() throws IOException {
		System.out.println("Closing all open connections.");
		
	}
}
