/*
 * 
 * __________________  DwellSMART CONFIDENTIAL __________________
 * 
 * (C) DwellSMART Pvt. Ltd. [2015] - All Rights Reserved.
 * 
 * NOTICE: All information contained herein is, and remains the property of DwellSMART Pvt. Ltd. 
 * and its partners, if any. The intellectual and technical concepts contained herein are 
 * proprietary to DwellSMART Pvt. Ltd. and its suppliers and may be covered by Patents,
 * patents in process, and are protected by trade secret or copyright law. 
 * Dissemination of this information or reproduction of this material is strictly forbidden 
 * unless prior written permission is obtained from DwellSMART Pvt. Ltd.
 * October 2015
 */
package javaapplication;

/**
 *
 * @author aryap
 */
import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import software.amazon.awssdk.crt.mqtt.MqttClientConnection;
import software.amazon.awssdk.crt.mqtt.MqttMessage;
import software.amazon.awssdk.crt.mqtt.QualityOfService;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import javax.ejb.LocalBean;

import software.amazon.awssdk.iot.AwsIotMqttConnectionBuilder;
//import com.dwellsmart.constants.DwellSmartConstants;

import javax.ejb.Stateless;

//@Stateless
//@LocalBean
public class MqttWrapper {

    private MqttClientConnection mqttClientConnection;
    private String requestTopic = "sdk/test/java/req";
    private String responseTopic = "sdk/test/java/res";
    
    String ENDPOINT = "aerlr7hitzxe4-ats.iot.ap-south-1.amazonaws.com";
    String CAPATH = "C:/Users/Admin/Downloads/MqttB/root-CA.crt";
    String ClientID = "basestation1";
    String CERTPATH = "C:/Users/Admin/Downloads/MqttB/MqttBroker.cert.pem";
    String KEYPATH = "C:/Users/Admin/Downloads/MqttB/MqttBroker.private.key";
    // Constructor
    public MqttWrapper() {
        this.mqttClientConnection = createConnection();
    }

    // Method to create MQTT connection using mTLS
    private MqttClientConnection createConnection() {
        try {
            // Build the connection using the mTLS builder from paths
            MqttClientConnection mqttConnection = AwsIotMqttConnectionBuilder.newMtlsBuilderFromPath(CERTPATH, KEYPATH)
                    .withEndpoint(ENDPOINT) // Set the endpoint
                    .withPort(8883) // Standard MQTT secure port
                    .withClientId(ClientID) // Set the client ID
                    .withCertificateAuthorityFromPath(null, CAPATH) // Set the CA path (if applicable)
                    .withCleanSession(true) // Use clean session
                    .withProtocolOperationTimeoutMs(60000) // Timeout after 60 seconds
                    .build();

            // Establish the connection asynchronously
            mqttConnection.connect().get();
            System.out.println("Connected to AWS IoT Core.");
            // Block and wait for the connection to complete

            // Return the established connection
            return mqttConnection;

        } catch (Exception e) {
            e.printStackTrace();
        }
        return null;
    }

    /**
     * Set the projectId and dynamically generate topics.
     *
     * @param projectId The ID of the project.
     */
    public void setProjectId(String projectId, String ipAddress) {
        this.requestTopic = "sdk/test/java/req";
        this.responseTopic ="sdk/test/java/res";
    }

    /**
     * Publish a message to the request topic.
     *
     * @param payload The payload to be published.
     * @return A CompletableFuture indicating the status of the publish.
     */
    public CompletableFuture<Integer> publishRequest(Map<String, String> data) {
        System.out.println("reqTopic " + requestTopic);

        if (requestTopic == null) {
            throw new IllegalStateException("Request topic is not set. Call setProjectId() first.");
        }

//        System.out.println("2sout");

        // Create the payload JSON object
        JsonObject payload = new JsonObject();
        payload.addProperty("messageId", "test1");
        payload.addProperty("opType", "READ");
        payload.addProperty("ipAddress", "103.190.0.100:100");
        
        List<Map<String, String>> meters = new ArrayList<>();
        Map<String, String> map = new HashMap<>();
        map.put("meterId","1");
        map.put("metersTypeId","9");
        meters.add(map);
        
        
        Map<String, String> map1 = new HashMap<>();
        map1.put("meterId","170");
        map1.put("metersTypeId","9");
        meters.add(map1);

        // Create meters array
        JsonArray metersArray = new JsonArray();
        for (Map<String, String> meter : meters) {
            JsonObject meterObject = new JsonObject();

            // Add meterId and metersTypeId
            meterObject.addProperty("meterId", meter.get("meterId"));
            meterObject.addProperty("metersTypeId", meter.get("MeterTypeId"));

            // Add the data object to each meter
            JsonObject dataObject = new JsonObject();
            for (Map.Entry<String, String> entry : data.entrySet()) {
                dataObject.addProperty(entry.getKey(), entry.getValue());
            }
//            if (opType.equals("SETLOAD")) {
//
//                meterObject.add("data", dataObject);
//            }

            metersArray.add(meterObject);
        }

        payload.add("meters", metersArray);
        System.out.println("payLoad " + payload.toString());
        // Publish the message
        MqttMessage message = new MqttMessage(
                requestTopic,
                payload.toString().getBytes(StandardCharsets.UTF_8),
                QualityOfService.AT_LEAST_ONCE
        );

//        System.out.println("3sout");
        System.out.println("mqttconn " + mqttClientConnection);

        return mqttClientConnection.publish(message);
    }

    /**
     * Subscribe to the response topic and handle incoming messages.
     *
     * @param messageHandler A callback function to process received messages.
     * @return A CompletableFuture indicating the status of the subscription.
     */
    public CompletableFuture<Void> subscribeResponse(java.util.function.Consumer<String> messageHandler) {
        if (responseTopic == null) {
            throw new IllegalStateException("Response topic is not set. Call setProjectId() first.");
        }

        try {
            // Subscribe to the response topic
            System.out.println("Subscribing to response topic: " + responseTopic);
            return mqttClientConnection.subscribe(responseTopic, QualityOfService.AT_LEAST_ONCE, (message) -> {
                // Handle incoming message
                String payload = new String(message.getPayload(), StandardCharsets.UTF_8);
                System.out.println("Message received on topic: " + message.getTopic());
                System.out.println("Payload: " + payload);

                // Pass the payload to the provided handler
                messageHandler.accept(payload);
            }).thenRun(() -> System.out.println("Subscription successful to topic: " + responseTopic));
        } catch (Exception e) {
            
            System.err.println("Error while subscribing to topic: " + e.getMessage());
            CompletableFuture<Void> failedFuture = new CompletableFuture<>();
            failedFuture.completeExceptionally(e);
            return failedFuture;
        }
    }

    public CompletableFuture<Void> close() {
        try {
            System.out.println("Disconnecting and closing connection...");

            // First, disconnect the connection asynchronously
            return mqttClientConnection.disconnect().thenRun(() -> {
                // After disconnecting, close the connection to release resources
                mqttClientConnection.close();
                System.out.println("Connection closed.");
            });
        } catch (Exception e) {
            System.err.println("Error while disconnecting and closing: " + e.getMessage());
            // Create a CompletableFuture and complete it exceptionally
            CompletableFuture<Void> failedFuture = new CompletableFuture<>();
            failedFuture.completeExceptionally(e);
            return failedFuture;
        }
    }

    // Getter for the request topic
    public String getRequestTopic() {
        return requestTopic;
    }

    // Getter for the response topic
    public String getResponseTopic() {
        return responseTopic;
    }
}