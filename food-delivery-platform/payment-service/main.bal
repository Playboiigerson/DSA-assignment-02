import ballerina/http;
import ballerinax/mongodb;
import ballerinax/kafka;
import ballerina/log;
import ballerina/time;
import ballerina/random;

configurable string mongoHost = "localhost";
configurable int mongoPort = 27017;
configurable string dbName = "payment_db";
configurable string kafkaBootstrap = "localhost:9092";

configurable string customerServiceUrl = "http://localhost:9001/customers";

public type OrderCreatedEvent record {
    string orderId;
    string customerId;
    string restaurantId;
    decimal amount;
};

public type PaymentCompletedEvent record {|
    string orderId;
    string customerId;
    decimal amount;
    string status; // SUCCESS or FAILED
    string reason;
|};

public type PaymentTransaction record {|
    string transactionId;
    string orderId;
    string customerId;
    decimal amount;
    string status;
    string timestamp;
|};

public type BalancePayload record {|
    decimal amount;
|};

final mongodb:Client mongoClient;
final mongodb:Collection paymentCollection;
final kafka:Producer kafkaProducer;
final http:Client customerClient;

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
    paymentCollection = check db->getCollection("payments");

    kafkaProducer = check new (kafkaBootstrap, {
        clientId: "payment-service-producer"
    });

    customerClient = check new (customerServiceUrl);

    log:printInfo("Payment Service initialized. Connecting to customer-service at " + customerServiceUrl);
}

// Kafka Listener to consume 'orders.created'
listener kafka:Listener createdListener = new (kafkaBootstrap, {
    groupId: "payment-service-group",
    topics: ["orders.created"]
});

service on createdListener {
    remote function onConsumerRecord(kafka:BytesConsumerRecord[] records) returns error? {
        foreach var rec in records {
            string payloadStr = check string:fromBytes(rec.value);
            log:printInfo("Payment Service received orders.created event: " + payloadStr);
            
            json payloadJson = check payloadStr.fromJsonString();
            OrderCreatedEvent event = check payloadJson.cloneWithType(OrderCreatedEvent);
            
            check processPayment(event);
        }
    }
}

function processPayment(OrderCreatedEvent event) returns error? {
    log:printInfo(string `Processing payment of ${event.amount} for customer ${event.customerId}, order ${event.orderId}`);
    
    string status = "FAILED";
    string reason = "";
    
    do {
        // Call Customer Service to deduct balance
        BalancePayload payload = { amount: event.amount };
        http:Response response = check customerClient->post(string `/${event.customerId}/deduct-balance`, payload);
        
        if response.statusCode == http:STATUS_OK {
            status = "SUCCESS";
            log:printInfo(string `Payment successful for order ${event.orderId}`);
        } else if response.statusCode == http:STATUS_BAD_REQUEST {
            reason = "Insufficient funds";
            log:printWarn(string `Payment failed for order ${event.orderId}: Insufficient funds`);
        } else if response.statusCode == http:STATUS_NOT_FOUND {
            reason = "Customer not found";
            log:printWarn(string `Payment failed for order ${event.orderId}: Customer not found`);
        } else {
            reason = "HTTP error code " + response.statusCode.toString();
            log:printError(string `Payment failed for order ${event.orderId}: ${reason}`);
        }
    } on fail error err {
        reason = "System error: " + err.message();
        log:printError(string `Payment failed for order ${event.orderId} due to internal error: ${err.message()}`);
    }
    
    // Save transaction
    string txnId = "txn_" + check random:createDecimal().toString().substring(2, 10);
    string timestamp = time:utcToString(time:utcNow());
    PaymentTransaction paymentTxn = {
        transactionId: txnId,
        orderId: event.orderId,
        customerId: event.customerId,
        amount: event.amount,
        status: status,
        timestamp: timestamp
    };
    
    do {
        check paymentCollection->insertOne(paymentTxn);
        log:printInfo("Saved payment transaction: " + txnId);
    } on fail error err {
        log:printError("Failed to save payment transaction: " + err.message());
    }
    
    // Publish payments.completed event
    PaymentCompletedEvent completedEvent = {
        orderId: event.orderId,
        customerId: event.customerId,
        amount: event.amount,
        status: status,
        reason: reason
    };
    
    json eventJson = completedEvent.toJson();
    check kafkaProducer->send({
        topic: "payments.completed",
        value: eventJson.toString().toBytes()
    });
    log:printInfo("Published payments.completed event for " + event.orderId);
}
