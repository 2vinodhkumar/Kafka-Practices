import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import twitter4j.FilterQuery;
import twitter4j.StallWarning;
import twitter4j.Status;
import twitter4j.StatusDeletionNotice;
import twitter4j.StatusListener;
import twitter4j.Twitter;
import twitter4j.TwitterFactory;
import twitter4j.TwitterObjectFactory;
import twitter4j.TwitterStream;
import twitter4j.TwitterStreamFactory;
import twitter4j.conf.ConfigurationBuilder;
import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;


public class TwitterProducer {

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		TwitterProducer tp = new TwitterProducer();
		tp.startTweets();
	}
	public void startTweets(){
		
		//Kafka prop
		
		Properties props= new Properties();
		
		props.put("metadata.broker.list", "localhost:9092");
		props.put("serializer.class", "kafka.serializer.StringEncoder");
		props.put("request.required.acks", "1");
		
		ProducerConfig config =new ProducerConfig(props);
		
		//twitter prop
		final Producer<String,String> producer =new Producer<String,String>(config);
		String consumerKey = new String("key");
		String consumerSecret= new String("key");
		String accessToken= new String("1134238220-key");
		String accessTokenSecret = new String("key");
		
		ConfigurationBuilder cfb= new ConfigurationBuilder();
		
		cfb.setOAuthAccessToken(accessToken);
		cfb.setOAuthAccessTokenSecret(accessTokenSecret);
		cfb.setOAuthConsumerKey(consumerKey);
		cfb.setOAuthConsumerSecret(consumerSecret);
		
		cfb.setJSONStoreEnabled(true);
		cfb.setIncludeEntitiesEnabled(true);
		
		
		TwitterStream twitterStream = new TwitterStreamFactory(cfb.build()).getInstance();
		
		Map<String, String> headers = new HashMap<String, String>();
		
		
		StatusListener listener = new StatusListener() {
			// The onStatus method is executed every time a new tweet comes
			// in.
			public void onStatus(Status status) {
			    // The EventBuilder is used to build an event using the
			    // the raw JSON of a tweet
			    //logger.info(status.getUser().getScreenName() + ": " + status.getText());
			
				
				//kafka relate code
			    @SuppressWarnings("deprecation")
				KeyedMessage<String, String> data = new KeyedMessage<String, String>("MyTopic"
												 , TwitterObjectFactory.getRawJSON(status));
			   System.out.println(status);
			    producer.send(data);
			    
			    
			    //kafka code ends
			    
			  
			    
			}
			    
			public void onDeletionNotice(StatusDeletionNotice statusDeletionNotice) {}
			
			public void onTrackLimitationNotice(int numberOfLimitedStatuses) {}
			
			public void onScrubGeo(long userId, long upToStatusId) {}
			
			public void onException(Exception ex) {
			    
			    
			}
			
			public void onStallWarning(StallWarning warning) {}
		    };
		
		    twitterStream.addListener(listener);
		    FilterQuery filterQuery= new FilterQuery();
		    String[] keyWords= {"Jammu","Kasmir","War"};
		    filterQuery.track(keyWords);
		 		   // twitterStream.sample();
		    twitterStream.filter(filterQuery);
	}

}
