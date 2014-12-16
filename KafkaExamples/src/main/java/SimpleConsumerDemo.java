import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;

import kafka.javaapi.message.ByteBufferMessageSet;

import kafka.message.MessageAndOffset;


public class SimpleConsumerDemo {

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		generateData();
		
	}
	private static void printMessages(ByteBufferMessageSet messageSet) throws UnsupportedEncodingException {
	    for(MessageAndOffset messageAndOffset: messageSet) {
	      ByteBuffer payload = messageAndOffset.message().payload();
	      byte[] bytes = new byte[payload.limit()];
	      payload.get(bytes);
	      System.out.println(new String(bytes, "UTF-8"));
	    }
	  }
	private static void generateData() {
	    Producer producer2 = new Producer("topic2");
	    producer2.start();
	   /* Producer producer3 = new Producer("topic3");
	    producer3.start(); */
	    try {
	      Thread.sleep(1000);
	    } catch (InterruptedException e) {
	      e.printStackTrace();
	    }
	  }
	

}
