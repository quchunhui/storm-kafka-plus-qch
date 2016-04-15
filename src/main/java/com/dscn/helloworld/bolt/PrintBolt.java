package com.dscn.helloworld.bolt;

import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;

@SuppressWarnings("serial")
public class PrintBolt extends BaseBasicBolt {
    public void execute(Tuple input, BasicOutputCollector collector) {
        String sentence = input.getString(0);
    	System.out.println("PrintBolt sentence=" + sentence);
    }

    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("word"));
    }
}
