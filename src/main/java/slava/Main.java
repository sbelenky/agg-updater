package slava;

import java.io.IOException;
import java.util.Properties;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.Consumer;
import com.rabbitmq.client.DefaultConsumer;
import com.rabbitmq.client.Envelope;

public class Main {

	private static final Logger LOG = LoggerFactory.getLogger(Main.class);

	public static void main(String[] args) throws Exception {
		LOG.info("Starting Agg Updater");

		Properties props = new Properties();
		props.load(ClassLoader.getSystemResourceAsStream("queue.properties"));

		ConnectionFactory factory = new ConnectionFactory();
		factory.setHost(props.getProperty("rabbitmq.host"));
		factory.setPort(Integer.parseInt(props.getProperty("rabbitmq.port")));
		factory.setUsername(props.getProperty("rabbitmq.username"));
		factory.setPassword(props.getProperty("rabbitmq.password"));
		factory.setVirtualHost(props.getProperty("rabbitmq.vhost"));

		Connection connection = factory.newConnection();
		Channel channel = connection.createChannel();

		Consumer consumer = new DefaultConsumer(channel) {
			@Override
			public void handleDelivery(String consumerTag, Envelope envelope, AMQP.BasicProperties properties,
					byte[] body) throws IOException {
				String message = new String(body, "UTF-8");
				LOG.info("Received '" + message + "'");
			}
		};

		channel.basicConsume(props.getProperty("rabbitmq.queue"), consumer);
	}

}
