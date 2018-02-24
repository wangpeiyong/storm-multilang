package cn.seeking.multilang;

import org.apache.storm.spout.ShellSpout;
import org.apache.storm.topology.IRichSpout;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;

import java.util.List;
import java.util.Map;

/**
 * @Author wangpeiyong
 * @Date 2018/1/15 11:26
 */
public class ScriptSpout extends ShellSpout implements IRichSpout {

	protected List<String> fields;

	public ScriptSpout(String command, String codeResource, List<String> fields) {
		super(command, codeResource);

		this.fields = fields;
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields(fields));
	}

	@Override
	public Map<String, Object> getComponentConfiguration() {
		return null;
	}

}