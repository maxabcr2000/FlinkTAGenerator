package max.flink.tagenerator;

import java.util.Properties;

import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010;
import org.apache.flink.streaming.util.serialization.SimpleStringSchema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 
 *
 */
public class TAGeneratorMain 
{
	private static final Logger LOG = LoggerFactory.getLogger(TAGeneratorMain.class);
	
    public static void main( String[] args ) throws Exception
    {
    	    	
    	//Kafka Properties
    	Properties properties = new Properties();
    	properties.setProperty("bootstrap.servers", "localhost:9092");
    	properties.setProperty("group.id", "test");
//    	
//    	
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
       
        try {
        	//TypeInformationKeyValueSerializationSchema<String, String> schema = new TypeInformationKeyValueSerializationSchema<String, String>(String.class, String.class, env.getConfig());
        	
        	FlinkKafkaConsumer010<String> kafkaSrc = new FlinkKafkaConsumer010<String>("test", new SimpleStringSchema(), properties);
        	kafkaSrc.assignTimestampsAndWatermarks(new AscendingTimestampExtractor<String>() {
        	    @Override
        	    public long extractAscendingTimestamp(String s) {
        	    	//extract time value from message
        	        return Long.valueOf(StringUtils.split(s,"_")[0]);
        	    }
        	});
        	
	        DataStream<String> dataStream = env.addSource(kafkaSrc);
	        dataStream.map(new MapFunction<String, Double>() {
	        	public Double map (String s) throws Exception{
	        		//extract price value from message
	        		return Double.valueOf(StringUtils.split(s,"_")[1]);
	        	}
	        })
	        	.windowAll(SlidingEventTimeWindows.of(Time.seconds(21), Time.seconds(5)))
	        	//.trigger(CountTrigger.of(5))
	        	.reduce(new ReduceFunction<Double>() {
	        		public Double reduce(Double v1, Double v2) throws Exception{
	        	        return v1+v2;
	        	    }
	        	})
	        	.map(new MapFunction<Double, Double>() {
	            	public Double map (Double d) throws Exception{
	            		return d/5;
	            	}
	            }).print();
	        
	        env.execute("TAGenerator");
        }catch(Exception e) {
        	LOG.error(e.getMessage(), e);
        }
    }
}
