package com.dscn.helloworld.bolt;

import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;

@SuppressWarnings("serial")
public class PrintBolt extends BaseBasicBolt {
    public void execute(Tuple input, BasicOutputCollector collector) {
    	System.out.println("PrintBolt execute.");
    }

    public void declareOutputFields(OutputFieldsDeclarer declarer) {
    	System.out.println("PrintBolt declareOutputFields.");
        declarer.declare(new Fields("word"));
    }
}
