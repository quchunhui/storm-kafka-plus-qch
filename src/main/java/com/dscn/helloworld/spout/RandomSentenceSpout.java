package com.dscn.helloworld.spout;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import storm.kafka.DynamicPartitionConnections;
import storm.kafka.KafkaUtils;
import storm.kafka.PartitionCoordinator;
import storm.kafka.PartitionManager;
import storm.kafka.SpoutConfig;
import storm.kafka.ZkCoordinator;
import storm.kafka.ZkState;

import java.util.*;

@SuppressWarnings("serial")
public class RandomSentenceSpout extends BaseRichSpout  {
    static enum EmitState {
        EMITTED_MORE_LEFT,
        EMITTED_END,
        NO_EMITTED
    };

    public final String TRANSACTIONAL_ZOOKEEPER_ROOT="transactional.zookeeper.root";
    public final String TRANSACTIONAL_ZOOKEEPER_SERVERS="transactional.zookeeper.servers";
    public final String TRANSACTIONAL_ZOOKEEPER_PORT="transactional.zookeeper.port";

    @SuppressWarnings("unused")
	private SpoutOutputCollector _collector;
    private SpoutConfig _spoutConfig;
    private PartitionCoordinator _coordinator;
    private DynamicPartitionConnections _connections;
    private ZkState _state;

    int _currPartitionIndex = 0;
    long _lastUpdateMs = 0;

    public RandomSentenceSpout(SpoutConfig spoutConfig) {
    	_spoutConfig = spoutConfig;
    }

    @SuppressWarnings({ "unchecked", "rawtypes" })
    public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
    	_collector = collector;
    	String topologyInstanceId = context.getStormId();

    	Map stateConf = new HashMap(conf);
        List<String> zkServers = _spoutConfig.zkServers;
        Integer zkPort = _spoutConfig.zkPort;
        stateConf.put(TRANSACTIONAL_ZOOKEEPER_SERVERS, zkServers);
        stateConf.put(TRANSACTIONAL_ZOOKEEPER_PORT, zkPort);
        stateConf.put(TRANSACTIONAL_ZOOKEEPER_ROOT, _spoutConfig.zkRoot);
        _state = new ZkState(stateConf);
    	    	
    	_connections = new DynamicPartitionConnections(
    			_spoutConfig, KafkaUtils.makeBrokerReader(conf, _spoutConfig));

    	int totalTasks = context.getComponentTasks(context.getThisComponentId()).size();
    	_coordinator = new ZkCoordinator(
    			_connections, conf, _spoutConfig, _state, context.getThisTaskIndex(), totalTasks, topologyInstanceId);
    }

    public void nextTuple() {
    	long diffWithNow = System.currentTimeMillis() - _lastUpdateMs;

    	if (diffWithNow > _spoutConfig.stateUpdateIntervalMs || diffWithNow < 0) {
            commit();
        }
    }

    public void ack(Object id) {
    	
    }

    public void fail(Object id) {
    	
    }

    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("word"));
    }

    private void commit() {
        _lastUpdateMs = System.currentTimeMillis();

        for (PartitionManager manager : _coordinator.getMyManagedPartitions()) {
            manager.commit();
        }
    }
}