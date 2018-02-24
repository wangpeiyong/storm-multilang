package cn.seeking.multilang;

import org.apache.storm.spout.Scheme;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.Charset;
import java.util.List;

public class MessageScheme implements Scheme {

    private Logger logger = LoggerFactory.getLogger(MessageScheme.class);

    public String field;

    public MessageScheme() {

    }

    public MessageScheme(String field) {
        this.field = field;
    }

    @Override
    public List<Object> deserialize(ByteBuffer ser) {
        String msg = null;
        try {
            msg = Charset.forName("UTF-8").newDecoder().decode(ser.asReadOnlyBuffer()).toString();
        } catch (CharacterCodingException e) {
            logger.error("deserialize with UTF-8 error:", e);
        }
        return new Values(msg);
    }

    @Override
    public Fields getOutputFields() {
        return new Fields(field);
    }
}
