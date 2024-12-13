package javaapplication;

import com.google.gson.JsonObject;
import software.amazon.awssdk.crt.mqtt.MqttClientConnection;
//import software.amazon.awssdk.crt.mqtt.MqttClientConnectionEvents;
import software.amazon.awssdk.crt.mqtt.MqttMessage;
import software.amazon.awssdk.crt.mqtt.QualityOfService;
import software.amazon.awssdk.iot.AwsIotMqttConnectionBuilder;

import java.nio.charset.StandardCharsets;
import java.util.concurrent.CompletableFuture;

public class PubSubServlet {
    public static void main(String[] args) {
        String endpoint = "aerlr7hitzxe4-ats.iot.ap-south-1.amazonaws.com";
        String clientId = "sdk-java";
        String id = "1";
        String topic = "sdk/test/java" +"/" + id;
        String certPath = "D:\\mqtt\\local_mqtt.cert.pem";
        String keyPath = "D:\\mqtt\\local_mqtt.private.key";
        String caRoot = "D:\\mqtt\\root-CA.crt";

        try {
            // MQTT Connection Setup
            AwsIotMqttConnectionBuilder builder = AwsIotMqttConnectionBuilder.newMtlsBuilderFromPath(certPath, keyPath)
                    .withEndpoint(endpoint)
                    .withClientId(clientId)
                    .withCleanSession(true)
                    .withProtocolOperationTimeoutMs(60000)
                    .withCertificateAuthorityFromPath(null, caRoot);

            MqttClientConnection connection = builder.build();
            CompletableFuture<Boolean> connected = connection.connect();
            connected.get();
            System.out.println("Connected to AWS IoT!");

            // Publish Message
            JsonObject message = new JsonObject();
            message.addProperty("deviceId", "device123");
            message.addProperty("message", "Hello from APJ!");
            connection.publish(new MqttMessage(
                    topic,
                    message.toString().getBytes(StandardCharsets.UTF_8),
                    QualityOfService.AT_LEAST_ONCE,
                    false
            )).get();
            System.out.println("Message published: " + message);
            
            // Disconnect
            connection.disconnect().get();
            System.out.println("Disconnected from AWS IoT.");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
