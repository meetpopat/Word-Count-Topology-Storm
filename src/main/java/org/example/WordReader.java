package org.example;

import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.util.Map;

public class WordReader extends BaseRichSpout {

    private SpoutOutputCollector collector;
    private FileReader fileReader;

    //setup buffer reader to iterate through the lines in the file
    private BufferedReader reader;

    private boolean completed = false;

    private String str;
    @Override
    public void open(Map<String, Object> map, TopologyContext topologyContext, SpoutOutputCollector spoutOutputCollector) {
        try {
            this.fileReader = new FileReader(map.get("fileToRead").toString());
        }
        catch (FileNotFoundException e){
            throw new RuntimeException("Error Reading File [" + map.get("wordFile") +"] ");
        }

        this.collector = spoutOutputCollector;
        this.reader = new BufferedReader(fileReader);
    }

    @Override
    public void nextTuple() {
        if(!completed){
            try {
                this.str = reader.readLine();
                if(this.str!=null){
                    this.collector.emit(new Values(str));
                }
            }
            catch (Exception e){
                throw new RuntimeException("Error Reading tupple", e);
            }
        }

    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("line"));

    }
}
