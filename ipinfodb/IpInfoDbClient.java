package org.ipinfodb;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.deser.DeserializationProblemHandler;

import org.apache.http.HttpResponse;
import org.apache.http.HttpStatus;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.DefaultHttpClient;
import org.apache.http.protocol.BasicHttpContext;
import org.apache.http.util.EntityUtils;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;

/**
 * A simple Java client for the IpInfoDb API.
 * <br>
 * License: Free for use in any way.
 * <br>
 * <br>
 * This example is dependent on the following libraries:
 * <pre>
 * commons-logging-1.1.1.jar
 * httpclient-4.2.jar
 * httpcore-4.2.jar
 * jackson-annotations-2.1.0.jar
 * jackson-core-2.1.0.jar
 * jackson-databind-2.1.0.jar
 * </pre>
 * <br>
 *
 * Created with IntelliJ IDEA.
 * User: MosheElisha
 */
public class IpInfoDbClient {

    private static final HttpClient HTTP_CLIENT = new DefaultHttpClient();
    private static final ObjectMapper MAPPER = new ObjectMapper();
    static {
        // Add a handler to handle unknown properties (in case the API adds new properties to the response)
        MAPPER.addHandler(new DeserializationProblemHandler() {
            @Override
            public boolean handleUnknownProperty(DeserializationContext context, JsonParser jp, JsonDeserializer<?> deserializer, Object beanOrClass, String propertyName) throws IOException {
                // Do not fail - just log
                String className = (beanOrClass instanceof Class) ? ((Class) beanOrClass).getName() : beanOrClass.getClass().getName();
                System.out.println("Unknown property while de-serializing: " + className + "." + propertyName);
                context.getParser().skipChildren();
                return true;
            }
        });
    }

    public static void main(String[] args) throws IOException {
        if (args.length < 3 || args.length > 4) {
            System.out.println("Usage: org.ipinfodb.IpInfoDbClient MODE API_KEY [IP_ADDRESS]\n"
                    + "MODE can be either 'ip-country' or 'ip-city'.\n"
                    + "If you don't have an API_KEY yet, you can get one for free by registering at http://www.ipinfodb.com/register.php."
            );
            return;
        }
        
        //Code for Reading the multiple ip addresses from a given txt file.
        File inputLogFilePath = new File(
				args[1]);
		FileReader fr = new FileReader(inputLogFilePath);
		BufferedReader br = new BufferedReader(fr);
		String logLine;
		/*File outputFile=new File("C:/Users/Intelligrape/Desktop/New_Sample_29042014.txt");
		// if file doesnt exists, then create it
					if (!outputFile.exists()) {
						outputFile.createNewFile();
					}
		 
		FileWriter fw = new FileWriter(outputFile,true);
		BufferedWriter bw = new BufferedWriter(fw);
		bw.write("S.no, IPAddress, Country, Region, City, Zipcode");
		bw.newLine();*/
		
        try {
            Class.forName("com.mysql.jdbc.Driver");
            System.out.println("Driver found");
        } catch (ClassNotFoundException e) {
            System.out.println("Driver not found: "+e);
        }
        
       
        String url_db="jdbc:mysql://mysqldb.c7zitrf2gguq.us-east-1.rds.amazonaws.com/matse?user=shagun&password=shagun001";
        
        Connection con=null;
        
        int insertCounter=0;
		int count=1;
		
		while((logLine=br.readLine())!=null){
			
			String mode = args[2];
	        String apiKey = args[3];
	        String url = "http://api.ipinfodb.com/v3/" + mode + "/?format=json&key=" + apiKey;
	        String[] split=logLine.split(",");
	        String ip = split[3];
			url += "&ip=" + ip;
			
        try {
            HttpGet request = new HttpGet(url);
            HttpResponse response = HTTP_CLIENT.execute(request, new BasicHttpContext());
            if (response.getStatusLine().getStatusCode() != HttpStatus.SC_OK) {
                throw new RuntimeException("IpInfoDb response is " + response.getStatusLine());
            }

            String responseBody = EntityUtils.toString(response.getEntity());
            IpCityResponse ipCityResponse = MAPPER.readValue(responseBody, IpCityResponse.class);
            if ("OK".equals(ipCityResponse.getStatusCode())) {
                //System.out.print(count+", "+ip+", "+ipCityResponse.getCountryCode() + ", " + ipCityResponse.getRegionName() + ", " + ipCityResponse.getCityName() + ", " + ipCityResponse.zipCode+ ", " + ipCityResponse.countryName + ", " + ipCityResponse.latitude + ", " + ipCityResponse.longitude + ", " + ipCityResponse.timeZone);
                //bw.write(count+", "+ip+", "+ipCityResponse.getCountryCode() + ", " + ipCityResponse.getRegionName() + ", " + ipCityResponse.getCityName() + ", " + ipCityResponse.zipCode + ", " + ipCityResponse.countryName + ", " + ipCityResponse.latitude + ", " + ipCityResponse.longitude + ", " + ipCityResponse.timeZone);
            
          	  try {
                  con=DriverManager.getConnection(url_db);
                  //System.out.println("Connected successfully"); 
                  Statement stmt=con.createStatement();
                  //System.out.println(logLine);
                  //System.out.println(split.length);
                  int result=stmt.executeUpdate("INSERT INTO LOGDATA1 VALUES('"+split[0]+"','"+split[1].trim()+"','"+split[2]+"','"+split[3]+"','"+split[4]+"','"+ipCityResponse.statusCode+"','"+ipCityResponse.countryCode+"','"+ipCityResponse.countryName+"','"+ipCityResponse.regionName+"','"+ipCityResponse.cityName+"','"+ipCityResponse.zipCode+"','"+ipCityResponse.latitude+"','"+ipCityResponse.longitude+"','"+ipCityResponse.timeZone+"')");
                  ++insertCounter;
                  con.close();
                  System.out.println(insertCounter);
              } catch (SQLException e) {
                  System.out.println("something wrong in the connection string: "+e);    
              }
            
            } else {
                System.out.print("API status message is '" + ipCityResponse.getStatusMessage() + "'");   
            }
        } finally {
            
        	//HTTP_CLIENT.getConnectionManager().shutdown();
        	
        }
        ++count;

		}
		HTTP_CLIENT.getConnectionManager().shutdown();
		
		
    }

    /**
     * <pre>
     * Example request:
     * http://api.ipinfodb.com/v3/ip-city/?format=json&key=API_KEY&ip=IP_ADDRESS
     *
     * Example response:
     * {
     * 	"statusCode" : "OK",
     * 	"statusMessage" : "",
     * 	"ipAddress" : "74.125.45.100",
     * 	"countryCode" : "US",
     * 	"countryName" : "UNITED STATES",
     * 	"regionName" : "CALIFORNIA",
     * 	"cityName" : "MOUNTAIN VIEW",
     * 	"zipCode" : "94043",
     * 	"latitude" : "37.3861",
     * 	"longitude" : "-122.084",
     * 	"timeZone" : "-07:00"
     * }
     * </pre>
     */
    @SuppressWarnings("UnusedDeclaration")
    public static class IpCityResponse {

        private String statusCode;
        private String statusMessage;
        private String ipAddress;
        private String countryCode;
        private String countryName;
        private String regionName;
        private String cityName;
        private String zipCode;
        private String latitude;
        private String longitude;
        private String timeZone;

        public String getStatusCode() {
            return statusCode;
        }

        public void setStatusCode(String statusCode) {
            this.statusCode = statusCode;
        }

        public String getStatusMessage() {
            return statusMessage;
        }

        public void setStatusMessage(String statusMessage) {
            this.statusMessage = statusMessage;
        }

        public String getIpAddress() {
            return ipAddress;
        }

        public void setIpAddress(String ipAddress) {
            this.ipAddress = ipAddress;
        }

        public String getCountryCode() {
            return countryCode;
        }

        public void setCountryCode(String countryCode) {
            this.countryCode = countryCode;
        }

        public String getCountryName() {
            return countryName;
        }

        public void setCountryName(String countryName) {
            this.countryName = countryName;
        }

        public String getRegionName() {
            return regionName;
        }

        public void setRegionName(String regionName) {
            this.regionName = regionName;
        }

        public String getCityName() {
            return cityName;
        }

        public void setCityName(String cityName) {
            this.cityName = cityName;
        }

        public String getZipCode() {
            return zipCode;
        }

        public void setZipCode(String zipCode) {
            this.zipCode = zipCode;
        }

        public String getLatitude() {
            return latitude;
        }

        public void setLatitude(String latitude) {
            this.latitude = latitude;
        }

        public String getLongitude() {
            return longitude;
        }

        public void setLongitude(String longitude) {
            this.longitude = longitude;
        }

        public String getTimeZone() {
            return timeZone;
        }

        public void setTimeZone(String timeZone) {
            this.timeZone = timeZone;
        }

        @Override
        public String toString() {
            return "IpCityResponse{" +
                    "statusCode='" + statusCode + '\'' +
                    ", statusMessage='" + statusMessage + '\'' +
                    ", ipAddress='" + ipAddress + '\'' +
                    ", countryCode='" + countryCode + '\'' +
                    ", countryName='" + countryName + '\'' +
                    ", regionName='" + regionName + '\'' +
                    ", cityName='" + cityName + '\'' +
                    ", zipCode='" + zipCode + '\'' +
                    ", latitude='" + latitude + '\'' +
                    ", longitude='" + longitude + '\'' +
                    ", timeZone='" + timeZone + '\'' +
                    '}';
        }
    }

}
