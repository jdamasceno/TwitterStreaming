package jdamasceno.storm.bolt;

import java.text.SimpleDateFormat;
import java.util.Map;

import twitter4j.Status;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Tuple;

import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.Mongo;

public class MongoBolt implements IRichBolt {
	private Mongo mongo;
	private DB tweetsDB; 
	
	@Override
	public void prepare(Map stormConf, TopologyContext context,
			OutputCollector collector) {
		try {
			mongo = new Mongo();
			tweetsDB = mongo.getDB("tweets");
		} catch (Throwable t) {
			t.printStackTrace();
		}
	}

	@Override
	public void execute(Tuple input) {
		Status status = (Status) input.getValue(0);
		
		DBCollection tweetsCollection = tweetsDB.getCollection("tweets");
		
		BasicDBObject tweet = new BasicDBObject();

		tweet.put("date", new SimpleDateFormat("hh:mm:ss").format(status.getCreatedAt()));
		tweet.put("local",status.getUser().getLocation());
		tweet.put("user", status.getUser().getScreenName());
		tweet.put("message", status.getText());

        tweetsCollection.insert(tweet);
	}

	@Override
	public void cleanup() {
		mongo.close();
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {

	}
}
