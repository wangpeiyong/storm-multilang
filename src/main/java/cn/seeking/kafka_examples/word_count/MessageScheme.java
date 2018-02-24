package cn.seeking.kafka_examples.word_count;

import org.apache.storm.spout.Scheme;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;

import java.nio.ByteBuffer;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.Charset;
import java.util.List;

public class MessageScheme implements Scheme {

    /* long: serialVersionUID */
    private static final long serialVersionUID = -2262911437633112301L;

    public static final String field = "msg";

    @Override
    public List<Object> deserialize(ByteBuffer ser) {
        String msg = null;
        try {
            msg = Charset.forName("UTF-8").newDecoder().decode(ser.asReadOnlyBuffer()).toString();
        } catch (CharacterCodingException e) {
            e.printStackTrace();
        }
        return new Values(msg);
    }

    @Override
    public Fields getOutputFields() {
        return new Fields(field);
    }
}
