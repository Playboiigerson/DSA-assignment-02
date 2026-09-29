import ballerina/http;
import ballerinax/mongodb;
import ballerinax/kafka;
import ballerina/log;
import ballerina/time;

configurable string mongoHost = "localhost";
configurable int mongoPort = 27017;
configurable string dbName = "admin_db";
configurable string kafkaBootstrap = "localhost:9092";

public type RestaurantReport record {|
    string id; // restaurant ID
    int totalOrders;
    decimal totalRevenue;
    int totalPrepTimeSeconds;
    decimal avgPrepTimeSeconds;
|};

public type DeliveryReport record {|
    string id; // constant e.g. "global"
    int totalDeliveries;
    int totalDeliveryTimeSeconds;
    decimal avgDeliveryTimeSeconds;
|};

public type TrackedOrder record {
    string id; // order ID
    string restaurantId;
    decimal amount;
    string confirmedAt;
};

type OrderConfirmedEvent record {
    string orderId;
    string restaurantId;
    decimal amount;
};

type OrderReadyEvent record {
    string orderId;
    string restaurantId;
};

type DeliveryCompletedEvent record {
    string orderId;
    string deliveryId;
    string driverId;
    int durationSeconds;
};

type PaymentCompletedEvent record {
    string orderId;
    string customerId;
    decimal amount;
    string status;
};

final mongodb:Client mongoClient;
final mongodb:Collection restaurantReportCollection;
final mongodb:Collection deliveryReportCollection;
final mongodb:Collection trackedOrderCollection;

function init() returns error? {
    mongoClient = check new ({
        connection: {
            serverAddress: {
                host: mongoHost,
                port: mongoPort
            }
        }
    });
    mongodb:Database db = check mongoClient->getDatabase(dbName);
    restaurantReportCollection = check db->getCollection("restaurant_reports");
    deliveryReportCollection = check db->getCollection("delivery_reports");
    trackedOrderCollection = check db->getCollection("tracked_orders");

    log:printInfo("Admin Service initialized.");
}

service /admin/reports on new http:Listener(9006) {

    // GET /admin/reports/restaurants - Get all restaurant performance reports
    resource function get restaurants() returns RestaurantReport[]|http:InternalServerError {
        do {
            stream<record {}, error?> resultStream = check restaurantReportCollection->find({});
            RestaurantReport[] reports = [];
            check resultStream.forEach(function(record {} doc) {
                do {
                    RestaurantReport rep = check doc.cloneWithType(RestaurantReport);
                    reports.push(rep);
                } on fail error e {
                    log:printError("Error parsing restaurant report: " + e.message());
                }
            });
            return reports;
        } on fail error err {
            log:printError("Error fetching restaurant reports: " + err.message());
            return http:INTERNAL_SERVER_ERROR;
        }
    }

    // GET /admin/reports/deliveries - Get global delivery statistics
    resource function get deliveries() returns DeliveryReport|http:NotFound|http:InternalServerError {
        do {
            stream<record {}, error?> resultStream = check deliveryReportCollection->find({ "id": "global" });
            record {| record {} value; |}? result = check resultStream.next();
            check resultStream.close();
            if result is () {
                return http:NOT_FOUND;
            }
            return check result.value.cloneWithType(DeliveryReport);
        } on fail error err {
            log:printError("Error fetching delivery reports: " + err.message());
            return http:INTERNAL_SERVER_ERROR;
        }
    }
}

// Kafka Listener to aggregate events
listener kafka:Listener adminListener = new (kafkaBootstrap, {
    groupId: "admin-service-group",
    topics: ["payments.completed", "orders.confirmed", "orders.ready", "delivery.completed"]
});

service on adminListener {
    remote function onConsumerRecord(kafka:BytesConsumerRecord[] records) returns error? {
        foreach var rec in records {
            string payloadStr = check string:fromBytes(rec.value);
            log:printInfo("Admin Service received event: " + payloadStr);
            json payloadJson = check payloadStr.fromJsonString();
            string topic = rec.offset.partition.topic;
            
            if topic == "payments.completed" {
                PaymentCompletedEvent event = check payloadJson.cloneWithType(PaymentCompletedEvent);
                if event.status == "SUCCESS" {
                    check handlePaymentSuccess(event);
                }
            } else if topic == "orders.confirmed" {
                OrderConfirmedEvent event = check payloadJson.cloneWithType(OrderConfirmedEvent);
                check handleOrderConfirmed(event);
            } else if topic == "orders.ready" {
                OrderReadyEvent event = check payloadJson.cloneWithType(OrderReadyEvent);
                check handleOrderReady(event);
            } else if topic == "delivery.completed" {
                DeliveryCompletedEvent event = check payloadJson.cloneWithType(DeliveryCompletedEvent);
                check handleDeliveryCompleted(event);
            }
        }
    }
}

function handlePaymentSuccess(PaymentCompletedEvent event) returns error? {
    // Payment confirmed
}

function handleOrderConfirmed(OrderConfirmedEvent event) returns error? {
    string timeNow = time:utcToString(time:utcNow());
    
    // Save tracked order
    TrackedOrder tracked = {
        id: event.orderId,
        restaurantId: event.restaurantId,
        amount: event.amount,
        confirmedAt: timeNow
    };
    check trackedOrderCollection->insertOne(tracked);
    log:printInfo(string `Tracked order confirmed: ${event.orderId} (Amount: $${event.amount})`);
}

function handleOrderReady(OrderReadyEvent event) returns error? {
    // 1. Get tracked order to compute preparation time and revenue
    stream<record {}, error?> resultStream = check trackedOrderCollection->find({ "id": event.orderId });
    record {| record {} value; |}? result = check resultStream.next();
    check resultStream.close();
    
    if result is () {
        log:printWarn("Could not find tracked order for prep time calculation: " + event.orderId);
        return;
    }
    
    TrackedOrder tracked = check result.value.cloneWithType(TrackedOrder);
    time:Utc startUtc = check time:utcFromString(tracked.confirmedAt);
    time:Utc endUtc = time:utcNow();
    int prepTimeSeconds = <int>time:utcDiffSeconds(endUtc, startUtc);
    
    // Remove from tracked orders as prep is completed
    _ = check trackedOrderCollection->deleteOne({ "id": event.orderId });
    
    // 2. Update restaurant report
    stream<record {}, error?> reportStream = check restaurantReportCollection->find({ "id": event.restaurantId });
    record {| record {} value; |}? reportResult = check reportStream.next();
    check reportStream.close();
    
    if reportResult is () {
        // Create new report
        RestaurantReport newReport = {
            id: event.restaurantId,
            totalOrders: 1,
            totalRevenue: tracked.amount, 
            totalPrepTimeSeconds: prepTimeSeconds,
            avgPrepTimeSeconds: <decimal>prepTimeSeconds
        };
        check restaurantReportCollection->insertOne(newReport);
    } else {
        RestaurantReport currentReport = check reportResult.value.cloneWithType(RestaurantReport);
        int newTotalOrders = currentReport.totalOrders + 1;
        decimal newTotalRevenue = currentReport.totalRevenue + tracked.amount;
        int newTotalPrepTime = currentReport.totalPrepTimeSeconds + prepTimeSeconds;
        decimal newAvgPrepTime = <decimal>newTotalPrepTime / <decimal>newTotalOrders;
        
        _ = check restaurantReportCollection->updateOne(
            { "id": event.restaurantId },
            { set: { 
                "totalOrders": newTotalOrders, 
                "totalRevenue": newTotalRevenue,
                "totalPrepTimeSeconds": newTotalPrepTime, 
                "avgPrepTimeSeconds": newAvgPrepTime 
            } }
        );
    }
    log:printInfo(string `Updated restaurant stats for ${event.restaurantId}: orders=${1}, revenue=$${tracked.amount}, prepTime=${prepTimeSeconds}s`);
}

function handleDeliveryCompleted(DeliveryCompletedEvent event) returns error? {
    // Update global delivery stats
    stream<record {}, error?> reportStream = check deliveryReportCollection->find({ "id": "global" });
    record {| record {} value; |}? reportResult = check reportStream.next();
    check reportStream.close();
    
    if reportResult is () {
        // Create new global report
        DeliveryReport newReport = {
            id: "global",
            totalDeliveries: 1,
            totalDeliveryTimeSeconds: event.durationSeconds,
            avgDeliveryTimeSeconds: <decimal>event.durationSeconds
        };
        check deliveryReportCollection->insertOne(newReport);
    } else {
        DeliveryReport currentReport = check reportResult.value.cloneWithType(DeliveryReport);
        int newTotalDeliveries = currentReport.totalDeliveries + 1;
        int newTotalDeliveryTime = currentReport.totalDeliveryTimeSeconds + event.durationSeconds;
        decimal newAvgDeliveryTime = <decimal>newTotalDeliveryTime / <decimal>newTotalDeliveries;
        
        _ = check deliveryReportCollection->updateOne(
            { "id": "global" },
            { set: { 
                "totalDeliveries": newTotalDeliveries, 
                "totalDeliveryTimeSeconds": newTotalDeliveryTime, 
                "avgDeliveryTimeSeconds": newAvgDeliveryTime 
            } }
        );
    }
    log:printInfo(string `Updated global delivery stats: deliveryTime=${event.durationSeconds}s`);
}
