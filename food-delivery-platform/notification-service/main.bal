import ballerinax/kafka;
import ballerina/log;

configurable string kafkaBootstrap = "localhost:9092";

function init() returns error? {
    log:printInfo("Notification Service initialized. Listening on Kafka topics...");
}

// Kafka Listener to consume all order lifecycle topics
listener kafka:Listener notificationListener = new (kafkaBootstrap, {
    groupId: "notification-service-group",
    topics: [
        "orders.created", 
        "payments.completed", 
        "orders.confirmed", 
        "orders.preparing", 
        "orders.ready", 
        "delivery.assigned", 
        "delivery.completed"
    ]
});

service on notificationListener {
    remote function onConsumerRecord(kafka:BytesConsumerRecord[] records) returns error? {
        foreach var rec in records {
            string payloadStr = check string:fromBytes(rec.value);
            json payloadJson = check payloadStr.fromJsonString();
            string topic = rec.offset.partition.topic;
            
            match topic {
                "orders.created" => {
                    log:printInfo(string `[SMS/PUSH] To Customer: Your order ${check payloadJson.orderId.ensureType(string)} has been placed! Waiting for payment...`);
                }
                "payments.completed" => {
                    string status = check payloadJson.status.ensureType(string);
                    string orderId = check payloadJson.orderId.ensureType(string);
                    if status == "SUCCESS" {
                        log:printInfo(string `[EMAIL] To Customer: Payment for order ${orderId} successful. Total deducted: $${check payloadJson.amount.ensureType(decimal)}`);
                    } else {
                        log:printWarn(string `[SMS/PUSH] To Customer: Payment for order ${orderId} failed: ${check payloadJson.reason.ensureType(string)}`);
                    }
                }
                "orders.confirmed" => {
                    log:printInfo(string `[DASHBOARD] To Restaurant ${check payloadJson.restaurantId.ensureType(string)}: Order ${check payloadJson.orderId.ensureType(string)} is confirmed! Please start preparing.`);
                }
                "orders.preparing" => {
                    log:printInfo(string `[SMS/PUSH] To Customer: The restaurant is preparing your order ${check payloadJson.orderId.ensureType(string)}. Estimated prep time: ${check payloadJson.estimatedTime.ensureType(string)}`);
                }
                "orders.ready" => {
                    log:printInfo(string `[DASHBOARD] To Driver Pool: An order is ready for pickup!`);
                }
                "delivery.assigned" => {
                    string orderId = check payloadJson.orderId.ensureType(string);
                    string driverName = check payloadJson.driverName.ensureType(string);
                    log:printInfo(string `[SMS/PUSH] To Customer: Order ${orderId} is out for delivery. Driver ${driverName} is on the way!`);
                }
                "delivery.completed" => {
                    string orderId = check payloadJson.orderId.ensureType(string);
                    log:printInfo(string `[SMS/PUSH] To Customer: Your order ${orderId} has been successfully delivered. Enjoy your meal!`);
                }
                _ => {
                    log:printInfo(string `Notification Service received event on topic ${topic}: ${payloadStr}`);
                }
            }
        }
    }
}
