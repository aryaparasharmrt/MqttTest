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

//import com.dwellsmart.entities.MeterMap;
//import com.dwellsmart.entities.MeterReadings;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import javax.ejb.EJB;
//import com.dwellsmart.wrappers.MeterDisplayParameter;
import java.math.BigDecimal;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.logging.Level;
import java.util.logging.Logger;
import javax.ejb.LocalBean;
import javax.ejb.Stateless;
import javax.json.Json;
import javax.json.JsonArray;
import javax.json.JsonObject;
import javax.json.JsonReader;
import javax.json.JsonValue;

/**
 *
 * @author aryap
 */
@Stateless
@LocalBean
public class MqttUtilities {

    
    MqttWrapper mqttWrapper;

    public MqttUtilities() {
        this.mqttWrapper = new MqttWrapper();
    }

    public MqttUtilities(MqttWrapper mqttWrapper) {
        this.mqttWrapper = mqttWrapper;
    }



public void executeMqttOperationForMeterReading(Map<String, String> data) {
    try {
        if (mqttWrapper == null) {
            System.out.println("Null Value");
            return;
        }

        // CompletableFuture to wait for the status update
        CompletableFuture<Boolean> statusFuture = new CompletableFuture<>();

        // Step 1: Subscribe to the response topic
        mqttWrapper.subscribeResponse(payload -> {
            System.out.println("Received payload: " + payload);
            // Handle the response and complete the CompletableFuture
            boolean isResponseValid = handleResponseForMeterReadings(payload);
            statusFuture.complete(isResponseValid);
        });

        // Step 2: Publish the message
        CompletableFuture<Integer> publishFuture = mqttWrapper.publishRequest(data);

        // Wait for the response to be processed with a timeout of 15 seconds
        Boolean result = waitForMeterResponse(statusFuture);

        if (result != null && result) {
            System.out.println("Response received and processed successfully.");
        } else {
            System.out.println("Failed to receive or process the response.");
        }

    } catch (Exception e) {
        System.err.println("Error in MQTT operation: " + e.getMessage());
        e.printStackTrace();
    }
}

private boolean handleResponseForMeterReadings(String payload) {
    // Process the payload and return true if valid
    // Implement your response handling logic here
    System.out.println("Processing payload: " + payload);
    return true; // Example: returning true for successful processing
}

    private  Boolean waitForMeterResponse(CompletableFuture<Boolean> statusFuture) {
        try {
            // Wait for 15 seconds for the CompletableFuture to be completed
            return statusFuture.get(90, TimeUnit.SECONDS);
        } catch (Exception e) {
            System.err.println("Error while processing the response: " + e.getMessage());
        }
        return null; // Return null if an error or timeout occurs
    }

    private boolean waitForResponse(CompletableFuture<Boolean> statusFuture) {
        boolean result = false;

        try {
            result = statusFuture.get(150, TimeUnit.SECONDS); // Blocks until the lambda updates the CompletableFuture
            System.out.println("Final status[0] for SetLoad: " + result + " on Thread: " + Thread.currentThread().getName());
        } catch (TimeoutException e) {
            System.err.println("Timeout occurred while waiting for response on Thread: " + Thread.currentThread().getName());
            result = false;  // Or you can choose another default value
        } catch (Exception e) {
            System.err.println("Error while processing the response: " + e.getMessage());
        }
        return result;
    }
    
    public static void main(String[] args) {
        
        MqttUtilities mqttUtilities = new MqttUtilities();
        Map<String, String> data = new HashMap<>();
        data.put("1", "1");
        mqttUtilities.executeMqttOperationForMeterReading(data);
    }

}