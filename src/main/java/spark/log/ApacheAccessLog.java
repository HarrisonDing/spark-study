package spark.log;

import java.io.Serializable;

/**
 * Issue and fix: 1. Exception in thread "main" org.apache.spark.SparkException:
 * Task not serializable If create a class to encapsulate atomic operation Fixed
 * by : Add to implement from Serializable when create the class
 * 
 * @author Harrison.Ding
 *
 */
public class ApacheAccessLog implements Serializable {
	/**
	 * 
	 */
	private static final long	serialVersionUID	= 7733557435166653564L;
	private String				ipAddress;
	private String				clientIndentid;
	private String				userId;
	private String				dateTime;
	private String				method;
	private String				endPoint;									// target addresss
	private String				protocol;
	private Integer				responseCode;
	private Long				contentSize;

	public ApacheAccessLog(String ipAddress, String clientIndentid, String userId, String dateTime, String method,
			String endPoint, String protocol, Integer responseCode, Long contentSize) {
		super();
		this.ipAddress = ipAddress;
		this.clientIndentid = clientIndentid;
		this.userId = userId;
		this.dateTime = dateTime;
		this.method = method;
		this.endPoint = endPoint;
		this.protocol = protocol;
		this.responseCode = responseCode;
		this.contentSize = contentSize;
	}

	public String getIpAddress() {
		return ipAddress;
	}

	public void setIpAddress(String ipAddress) {
		this.ipAddress = ipAddress;
	}

	public String getClientIndentid() {
		return clientIndentid;
	}

	public void setClientIndentid(String clientIndentid) {
		this.clientIndentid = clientIndentid;
	}

	public String getUserId() {
		return userId;
	}

	public void setUserId(String userId) {
		this.userId = userId;
	}

	public String getDateTime() {
		return dateTime;
	}

	public void setDateTime(String dateTime) {
		this.dateTime = dateTime;
	}

	public String getMethod() {
		return method;
	}

	public void setMethod(String method) {
		this.method = method;
	}

	public String getEndPoint() {
		return endPoint;
	}

	public void setEndPoint(String endPoint) {
		this.endPoint = endPoint;
	}

	public String getProtocol() {
		return protocol;
	}

	public void setProtocol(String protocol) {
		this.protocol = protocol;
	}

	public Integer getResponseCode() {
		return responseCode;
	}

	public void setResponseCode(Integer responseCode) {
		this.responseCode = responseCode;
	}

	public Long getContentSize() {
		return contentSize;
	}

	public void setContentSize(Long contentSize) {
		this.contentSize = contentSize;
	}

	public ApacheAccessLog() {
		super();
	}

	public ApacheAccessLog parseLog(String log) {
		String[] logArray = log.split("#");
		if (logArray.length < 1) // not work well for empty line
			return null;
		String[] url = logArray[4].split(" ");

		return new ApacheAccessLog(logArray[0], logArray[1], logArray[2], logArray[3], url[0], url[1], url[2],
				Integer.valueOf(logArray[5]), Long.valueOf(logArray[6]));
	}
}
