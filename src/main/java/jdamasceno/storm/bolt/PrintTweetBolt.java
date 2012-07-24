package jdamasceno.storm.bolt;

import java.text.SimpleDateFormat;
import java.util.Map;

import twitter4j.Status;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Tuple;

public class PrintTweetBolt implements IRichBolt {

	OutputCollector collector;
	
	@Override
	public void prepare(Map stormConf, TopologyContext context,
			OutputCollector collector) {

	}

	@Override
	public void execute(Tuple input) {
		Status status = (Status) input.getValue(0);
		System.out.println(new SimpleDateFormat("hh:mm:ss").format(status.getCreatedAt()) +" "+status.getUser().getLocation()+" "+status.getUser().getScreenName()
				+ " " + status.getText());
	}

	@Override
	public void cleanup() {

	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {

	}
}
