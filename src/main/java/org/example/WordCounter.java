package org.example;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;

import java.io.PrintWriter;
import java.util.HashMap;
import java.util.Map;

public class WordCounter extends BaseRichBolt {

    Integer id;
    String name;
    Map<String,Integer> counters;
    String fileName;

    @Override
    public void prepare(Map<String, Object> map, TopologyContext topologyContext, OutputCollector outputCollector) {
        this.counters = new HashMap<String, Integer>();

        //Task ID and componentID of the bolt
        this.name = topologyContext.getThisComponentId();
        this.id = topologyContext.getThisTaskId();
        this.fileName = map.get("dirToWrite").toString() + "output" + "-" + topologyContext.getThisTaskId() + "-" + topologyContext.getThisComponentId() + ".txt";

        //should be unique as each instance will be written to different filr and hence we inculde taskID and componentID.

    }

    @Override
    public void execute(Tuple tuple) {
        String str = tuple.getString(0);
        if(!counters.containsKey(str)){
            counters.put(str,1);
        }
        else{
            int c = counters.get(str);
            counters.put(str,c+1);
        }

    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields());

    }
    public void cleanup(){
        try {
            PrintWriter writer = new PrintWriter(fileName, "UTF-8");
            for(Map.Entry<String,Integer> entry: counters.entrySet()){
                if(entry.getValue()>3){
                    writer.println(entry.getKey() + ":" + entry.getValue());
                }

            }
            writer.close();


        }
        catch (Exception e){

        }
    }

}
