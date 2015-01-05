package project.ebd;

import java.util.Map;
import java.util.Properties;

import javax.mail.Message;
import javax.mail.MessagingException;
import javax.mail.PasswordAuthentication;
import javax.mail.Session;
import javax.mail.Transport;
import javax.mail.internet.InternetAddress;
import javax.mail.internet.MimeMessage;

import org.json.simple.JSONObject;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Tuple;

public class AlertBolt implements IRichBolt{

	private static final long serialVersionUID = 1L;
	private OutputCollector collector;
	
	public void execute(Tuple input) {

		JSONObject jsonObj = new JSONObject();		
		jsonObj = (JSONObject) input.getValueByField("message");
		String alertType = "";

		if(!input.getValueByField("systemError").equals("")){
			alertType = (String) input.getValueByField("systemError");
		} else {
			if(!input.getValueByField("networkStatus").equals("")){
				alertType = (String) input.getValueByField("networkStatus");
			}
		}

		if(!alertType.equals("")){
			
			Properties props = new Properties();
			props.put("mail.smtp.auth", "true");
			props.put("mail.smtp.starttls.enable", "true");
			props.put("mail.smtp.host", "smtp.gmail.com");
			props.put("mail.smtp.port", "587");
			props.put("mail.smtp.user","LogsRealTime");

			/*
			props.put("mail.smtp.ssl.enable", "true");
			props.put("mail.smtp.EnableSSL.enable","true");
			props.setProperty("mail.smtp.socketFactory.class", "javax.net.ssl.SSLSocketFactory");  
			props.setProperty("mail.smtp.socketFactory.fallback", "false");  
			props.setProperty("mail.smtp.port", "465");  
			props.setProperty("mail.smtp.socketFactory.port", "465");
			*/
			
			Session session = Session.getInstance(props,
					new javax.mail.Authenticator() {
						protected PasswordAuthentication getPasswordAuthentication() {
							return new PasswordAuthentication(
									"logsrealtime@gmail.com", "logs1a2b3c");
						}
					});

			try {

				Message message = new MimeMessage(session);
				message.setFrom(new InternetAddress("logsrealtime@gmail.com"));
				message.setContent(jsonObj.toString(),"text/html"); 
				message.setRecipients(Message.RecipientType.TO,InternetAddress.parse("logsrealtime@gmail.com"));
				message.setSubject(alertType);
				Transport.send(message);

				System.out.println("Sent email: " + alertType );

			} catch (MessagingException e) {
				
			}
		}

		collector.ack(input);
		
	}

	@Override
	public void prepare(@SuppressWarnings("rawtypes") Map stormConf, TopologyContext context, OutputCollector outputCollector) {
		
		this.collector = outputCollector;
		
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer outputDeclarer) {
		
	}

	@Override
	public void cleanup() {

	}

	@Override
	public Map<String, Object> getComponentConfiguration() {
		// TODO Auto-generated method stub
		return null;
	}
}
