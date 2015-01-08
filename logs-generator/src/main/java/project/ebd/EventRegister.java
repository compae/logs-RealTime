package project.ebd;

import java.util.Date;

import com.google.gson.Gson;
import com.google.gson.JsonObject;

public class EventRegister {

	private final Date dateTimeLastPing;
	private final Date dateTimeEvent;
	private final String typeEvent;
	private final String application;
	private final String user;
	private final String ipHost;
	private final Integer cpuUse;
	private final Integer memoryUse;
	private final String networkStatus;
	private final Integer networkTraffic;
	private final String operatingSystem;
	private final String systemError;
	private final String country;
	private final String city;
	private final String company;
	private final String ipServer;
	private final String postalCode;
	private final String state;

	public EventRegister(Date dateTimeProcessed, Date dateTimeEvent,
			String typeEvent, String nameApplication, String nameUser, 
			String ipHost, Integer cpuUse, Integer memoryUse,
			String networkStatus, Integer networkTraffic,
			String operatingSystem, String systemError, String country,
			String city,String company, String ipServer, String postalCode, String state) {
		super();
		this.dateTimeLastPing = dateTimeProcessed;
		this.dateTimeEvent = dateTimeEvent;
		this.typeEvent = typeEvent;
		this.application = nameApplication;
		this.user = nameUser;
		this.company = company;
		this.ipHost = ipHost;
		this.cpuUse = cpuUse;
		this.memoryUse = memoryUse;
		this.networkStatus = networkStatus;
		this.networkTraffic = networkTraffic;
		this.operatingSystem = operatingSystem;
		this.systemError = systemError;
		this.country = country;
		this.city = city;
		this.ipServer =  ipServer;
		this.postalCode = postalCode;
		this.state = state;
	}
	
	public String JsonOutput(){
		
		Gson gson = new Gson();
		JsonObject eventObject = new JsonObject();
		eventObject.addProperty("dateTimeProcessed", dateTimeLastPing.toString());
		eventObject.addProperty("dateTimeEvent", dateTimeEvent.toString());
		eventObject.addProperty("typeEvent", typeEvent);
		eventObject.addProperty("application", application);
		eventObject.addProperty("user", user);
		eventObject.addProperty("ipHost", ipHost);
		eventObject.addProperty("cpuUse", cpuUse);
		eventObject.addProperty("memoryUse", memoryUse);
		eventObject.addProperty("networkStatus", networkStatus);
		eventObject.addProperty("networkTraffic", networkTraffic);
		eventObject.addProperty("operatingSystem", operatingSystem);
		eventObject.addProperty("systemError", systemError);
		eventObject.addProperty("country", country);
		eventObject.addProperty("city", city);
		eventObject.addProperty("company", company);
		eventObject.addProperty("ipServer", ipServer);
		eventObject.addProperty("postalCode", postalCode);
		eventObject.addProperty("state", state);
		
		return gson.toJson(eventObject);
	}



	public String getState() {
		return state;
	}

	public String getPostalCode() {
		return postalCode;
	}

	@Override
	public String toString() {
		return "{\"dateTimeLastPing\"=\"" + dateTimeLastPing
				+ "\", \"dateTimeEvent\"=\"" + dateTimeEvent
				+ "\", \"typeEvent\"=\"" + typeEvent + "\", \"application\"=\""
				+ application + "\", \"user\"=\"" + user + "\", \"ipHost\"=\""
				+ ipHost + "\", \"cpuUse\"=\"" + cpuUse
				+ "\", \"memoryUse\"=\"" + memoryUse
				+ "\", \"networkStatus\"=\"" + networkStatus
				+ "\", \"networkTraffic\"=\"" + networkTraffic
				+ "\", \"operatingSystem\"=\"" + operatingSystem
				+ "\", \"systemError\"=\"" + systemError + "\", \"country\"=\""
				+ country + "\", \"city\"=\"" + city + "\", \"company\"=\""
				+ company + "\", \"ipServer\"=\"" + ipServer
				+ "\", \"postalCode\"=\"" + postalCode + "\", \"state\"=\""
				+ state + "\"}";
	}


	public static EventRegister getInstance(Object[] args) {

		Date dateTimeLastPing = (Date) args[0];
		Date dateTimeEvent = (Date) args[1];
		String typeEvent = args[2].toString();
		String application = args[3].toString();
		String user = args[4].toString();
		String ipHost = args[5].toString();
		Integer cpuUse = (Integer) args[6];
		Integer memoryUse = (Integer) args[7];
		String networkStatus = args[8].toString();
		Integer networkTraffic = (Integer) args[9];
		String operatingSystem =  args[10].toString();
		String systemError = args[11].toString();
		String country = args[12].toString();
		String city = args[13].toString();
		String company = args[14].toString();
		String ipServer = args[15].toString();
		String postalCode = args[16].toString();
		String state = args[17].toString();

		return new EventRegister( dateTimeLastPing,  dateTimeEvent,  typeEvent,  application,
				user,  ipHost,  cpuUse,  memoryUse,  networkStatus, networkTraffic, operatingSystem, systemError, country, city, company, ipServer, postalCode, state);
		
	}
	
	public String getIpServer() {
		return ipServer;
	}

	public String getCompany() {
		return company;
	}

	public Date getDateTimeLastPing() {
		return dateTimeLastPing;
	}
	public Date getDateTimeEvent() {
		return dateTimeEvent;
	}
	public String getApplication() {
		return application;
	}

	public String getUser() {
		return user;
	}

	public String getIpHost() {
		return ipHost;
	}

	public Integer getNetworkTraffic() {
		return networkTraffic;
	}

	public String getOperatingSystem() {
		return operatingSystem;
	}

	public String getCountry() {
		return country;
	}

	public String getCity() {
		return city;
	}

	public String getTypeEvent() {
		return typeEvent;
	}
	public String getipHost() {
		return ipHost;
	}
	public Integer getCpuUse() {
		return cpuUse;
	}
	public Integer getMemoryUse() {
		return memoryUse;
	}
	public String getNetworkStatus() {
		return networkStatus;
	}
	public String getSystemError() {
		return systemError;
	}

}
