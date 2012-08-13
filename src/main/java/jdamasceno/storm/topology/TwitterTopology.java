package jdamasceno.storm.topology;

import jdamasceno.storm.bolt.MongoBolt;
import jdamasceno.storm.bolt.PrintTweetBolt;
import jdamasceno.storm.spout.TwitterSpout;
import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.InvalidTopologyException;
import backtype.storm.topology.TopologyBuilder;

public class TwitterTopology {

	/**
	 * @param args
	 * @throws InvalidTopologyException 
	 * @throws AlreadyAliveException 
	 */
	public static void main(String[] args) throws AlreadyAliveException, InvalidTopologyException {
		TopologyBuilder builder = new TopologyBuilder();
		
		TwitterSpout twitterSpout = new TwitterSpout();
		builder.setSpout("twitter", twitterSpout);
		
		builder.setBolt("mongo", new MongoBolt(), 2).shuffleGrouping("twitter");
//		builder.setBolt("print", new PrintTweetBolt(), 2).shuffleGrouping("twitter");
		
		Config conf = new Config();
        conf.setDebug(false);
        
        if(args!=null && args.length > 0) {
            conf.setNumWorkers(3);
            
            StormSubmitter.submitTopology(args[0], conf, builder.createTopology());
        } else {
	        LocalCluster cluster = new LocalCluster();
	        cluster.submitTopology("twitter", conf, builder.createTopology());
        }
		
	}

}
