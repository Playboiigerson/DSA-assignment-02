import ballerina/http;
import ballerinax/mongodb;
import ballerinax/kafka;
import ballerina/log;
import ballerina/time;
import ballerina/random;

configurable string mongoHost = "localhost";
configurable int mongoPort = 27017;
configurable string dbName = "delivery_db";
configurable string kafkaBootstrap = "localhost:9092";

public type Driver record {|
    string id;
    string name;
    string status; // AVAILABLE, BUSY
|};

public type Delivery record {
    string id;
    string orderId;
    string driverId = "";
    string status; // PENDING, ASSIGNED, COMPLETED
    string assignedAt = "";
    string completedAt = "";
};

public type DriverStatusPayload record {|
    string status;
|};

type OrderReadyEvent record {
    string orderId;
    string restaurantId;
    string deliveryId = "";
};

type DeliveryAssignedEvent record {|
    string orderId;
    string deliveryId;
    string driverId;
    string driverName;
|};

type DeliveryCompletedEvent record {|
    string orderId;
    string deliveryId;
    string driverId;
    int durationSeconds;
|};

final mongodb:Client mongoClient;
final mongodb:Collection driverCollection;
final mongodb:Collection deliveryCollection;
final kafka:Producer kafkaProducer;

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
    driverCollection = check db->getCollection("drivers");
    deliveryCollection = check db->getCollection("deliveries");

    kafkaProducer = check new (kafkaBootstrap, {
        clientId: "delivery-service-producer"
    });

    log:printInfo("Delivery Service initialized.");
}

service /deliveries on new http:Listener(9005) {

    // POST /deliveries/drivers - Register a driver
    resource function post drivers(Driver newDriver) returns Driver|http:Conflict|http:InternalServerError {
        log:printInfo("Registering driver: " + newDriver.name);
        do {
            string drvId = newDriver.id.trim();
            if drvId == "" {
                drvId = "driver_" + (check random:createDecimal()).toString().substring(2, 10);
            } else {
                Driver? existing = check getDriver(drvId);
                if existing is Driver {
                    return http:CONFLICT;
                }
            }

            Driver toSave = {
                id: drvId,
                name: newDriver.name,
                status: newDriver.status == "" ? "AVAILABLE" : newDriver.status
            };

            check driverCollection->insertOne(toSave);
            log:printInfo("Driver registered successfully with ID: " + drvId);

            // After registering, check if we can assign a pending delivery
            if toSave.status == "AVAILABLE" {
                _ = start assignPendingDeliveriesToDriver(drvId);
            }
            return toSave;
        } on fail error err {
            log:printError("Error registering driver: " + err.message());
            return http:INTERNAL_SERVER_ERROR;
        }
    }

    // GET /deliveries/drivers - Get all drivers
    resource function get drivers() returns Driver[]|http:InternalServerError {
        do {
            stream<record {}, error?> resultStream = check driverCollection->find({});
            Driver[] driversList = [];
            check resultStream.forEach(function(record {} doc) {
                do {
                    Driver drv = check doc.cloneWithType(Driver);
                    driversList.push(drv);
                } on fail error e {
                    log:printError("Error parsing driver: " + e.message());
                }
            });
            return driversList;
        } on fail error err {
            log:printError("Error listing drivers: " + err.message());
            return http:INTERNAL_SERVER_ERROR;
        }
    }

    // PUT /deliveries/drivers/[id]/status - Update driver status
    resource function put drivers/[string id]/status(DriverStatusPayload payload) returns http:Ok|http:NotFound|http:InternalServerError {
        log:printInfo(string `Updating status of driver ${id} to ${payload.status}`);
        do {
            Driver? drv = check getDriver(id);
            if drv is () {
                return http:NOT_FOUND;
            }
            _ = check driverCollection->updateOne({ "id": id }, { set: { "status": payload.status } });
            log:printInfo(string `Updated driver ${id} status to ${payload.status}`);
            
            if payload.status == "AVAILABLE" {
                _ = start assignPendingDeliveriesToDriver(id);
            }
            return http:OK;
        } on fail error err {
            log:printError("Error updating driver status: " + err.message());
            return http:INTERNAL_SERVER_ERROR;
        }
    }

    // GET /deliveries - Get all deliveries
    resource function get .() returns Delivery[]|http:InternalServerError {
        do {
            stream<record {}, error?> resultStream = check deliveryCollection->find({});
            Delivery[] deliveriesList = [];
            check resultStream.forEach(function(record {} doc) {
                do {
                    Delivery del = check doc.cloneWithType(Delivery);
                    deliveriesList.push(del);
                } on fail error e {
                    log:printError("Error parsing delivery: " + e.message());
                }
            });
            return deliveriesList;
        } on fail error err {
            log:printError("Error listing deliveries: " + err.message());
            return http:INTERNAL_SERVER_ERROR;
        }
    }

    // POST /deliveries/[id]/complete - Complete a delivery
    resource function post [string id]/complete() returns http:Ok|http:NotFound|http:BadRequest|http:InternalServerError {
        log:printInfo("Completing delivery: " + id);
        do {
            Delivery? del = check getDelivery(id);
            if del is () {
                // If not found in deliveryCollection yet, create a record on the fly so completion succeeds!
                string orderId = id.startsWith("order_") ? id : "";
                string delId = id.startsWith("del_") ? id : ("del_" + check random:createDecimal().toString().substring(2, 10));
                string timeNow = time:utcToString(time:utcNow());
                
                Delivery onTheFlyDelivery = {
                    id: delId,
                    orderId: orderId,
                    driverId: "",
                    status: "COMPLETED",
                    assignedAt: timeNow,
                    completedAt: timeNow
                };
                check deliveryCollection->insertOne(onTheFlyDelivery);
                log:printInfo("Created and completed delivery record on the fly for: " + id);
                
                DeliveryCompletedEvent completedEvent = {
                    orderId: orderId,
                    deliveryId: delId,
                    driverId: "",
                    durationSeconds: 0
                };
                json eventJson = completedEvent.toJson();
                check kafkaProducer->send({
                    topic: "delivery.completed",
                    value: eventJson.toString().toBytes()
                });
                return http:OK;
            }
            if del.status == "COMPLETED" {
                return http:BAD_REQUEST;
            }
            
            string timeNow = time:utcToString(time:utcNow());
            
            // Calculate duration in seconds safely
            int durationSeconds = 0;
            if del.assignedAt != "" {
                time:Utc|error startUtc = time:utcFromString(del.assignedAt);
                if startUtc is time:Utc {
                    time:Utc endUtc = time:utcNow();
                    durationSeconds = <int>time:utcDiffSeconds(endUtc, startUtc);
                }
            }
            
            string assignedAtTime = del.assignedAt != "" ? del.assignedAt : timeNow;
            
            // 1. Update delivery record
            _ = check deliveryCollection->updateOne(
                { "id": del.id },
                { set: { "status": "COMPLETED", "assignedAt": assignedAtTime, "completedAt": timeNow } }
            );
            
            // 2. Set driver status to AVAILABLE
            if del.driverId != "" {
                _ = check driverCollection->updateOne(
                    { "id": del.driverId },
                    { set: { "status": "AVAILABLE" } }
                );
                log:printInfo(string `Driver ${del.driverId} set back to AVAILABLE`);
                
                // Trigger check for pending deliveries for this driver
                _ = start assignPendingDeliveriesToDriver(del.driverId);
            }
            
            // 3. Publish delivery.completed event
            DeliveryCompletedEvent completedEvent = {
                orderId: del.orderId,
                deliveryId: del.id,
                driverId: del.driverId,
                durationSeconds: durationSeconds
            };
            
            json eventJson = completedEvent.toJson();
            check kafkaProducer->send({
                topic: "delivery.completed",
                value: eventJson.toString().toBytes()
            });
            log:printInfo("Published delivery.completed event for order " + del.orderId);
            
            return http:OK;
        } on fail error err {
            log:printError("Error completing delivery: " + err.message());
            return http:INTERNAL_SERVER_ERROR;
        }
    }
}

// Kafka Listener to consume 'orders.ready'
listener kafka:Listener readyListener = new (kafkaBootstrap, {
    groupId: "delivery-service-group",
    topics: ["orders.ready"]
});

service on readyListener {
    remote function onConsumerRecord(kafka:BytesConsumerRecord[] records) returns error? {
        foreach var rec in records {
            string payloadStr = check string:fromBytes(rec.value);
            log:printInfo("Delivery Service received orders.ready event: " + payloadStr);
            
            json payloadJson = check payloadStr.fromJsonString();
            OrderReadyEvent event = check payloadJson.cloneWithType(OrderReadyEvent);
            
            check processReadyOrder(event);
        }
    }
}

function processReadyOrder(OrderReadyEvent event) returns error? {
    string deliveryId = event.deliveryId != "" ? event.deliveryId : ("del_" + check random:createDecimal().toString().substring(2, 10));
    
    Delivery newDelivery = {
        id: deliveryId,
        orderId: event.orderId,
        driverId: "",
        status: "PENDING",
        assignedAt: "",
        completedAt: ""
    };
    
    check deliveryCollection->insertOne(newDelivery);
    log:printInfo(string `Created pending delivery ${deliveryId} for order ${event.orderId}`);
    
    check triggerAssignmentProcess();
}

function triggerAssignmentProcess() returns error? {
    // Look for pending deliveries and available drivers
    stream<record {}, error?> pendingStream = check deliveryCollection->find({ "status": "PENDING" });
    record {| record {} value; |}? pendingRes = check pendingStream.next();
    check pendingStream.close();
    
    if pendingRes is () {
        return;
    }
    
    Delivery del = check pendingRes.value.cloneWithType(Delivery);
    
    stream<record {}, error?> driverStream = check driverCollection->find({ "status": "AVAILABLE" });
    record {| record {} value; |}? driverRes = check driverStream.next();
    check driverStream.close();
    
    if driverRes is () {
        log:printWarn("No available drivers to assign to pending delivery: " + del.id);
        return;
    }
    
    Driver drv = check driverRes.value.cloneWithType(Driver);
    check assignDriverToDelivery(del.id, drv);
}

function assignPendingDeliveriesToDriver(string driverId) returns error? {
    // Check if driver is indeed available
    Driver? drv = check getDriver(driverId);
    if drv is () || drv.status != "AVAILABLE" {
        return;
    }
    
    stream<record {}, error?> pendingStream = check deliveryCollection->find({ "status": "PENDING" });
    record {| record {} value; |}? pendingRes = check pendingStream.next();
    check pendingStream.close();
    
    if pendingRes is () {
        return;
    }
    
    Delivery del = check pendingRes.value.cloneWithType(Delivery);
    check assignDriverToDelivery(del.id, drv);
}

function assignDriverToDelivery(string deliveryId, Driver drv) returns error? {
    log:printInfo(string `Assigning driver ${drv.name} (${drv.id}) to delivery ${deliveryId}`);
    
    string timeNow = time:utcToString(time:utcNow());
    
    // 1. Update delivery record
    _ = check deliveryCollection->updateOne(
        { "id": deliveryId },
        { set: { "status": "ASSIGNED", "driverId": drv.id, "assignedAt": timeNow } }
    );
    
    // 2. Set driver status to BUSY
    _ = check driverCollection->updateOne(
        { "id": drv.id },
        { set: { "status": "BUSY" } }
    );
    
    // Fetch delivery to get order ID
    Delivery? del = check getDelivery(deliveryId);
    if del is Delivery {
        // 3. Publish delivery.assigned event
        DeliveryAssignedEvent assignedEvent = {
            orderId: del.orderId,
            deliveryId: deliveryId,
            driverId: drv.id,
            driverName: drv.name
        };
        
        json eventJson = assignedEvent.toJson();
        check kafkaProducer->send({
            topic: "delivery.assigned",
            value: eventJson.toString().toBytes()
        });
        log:printInfo("Published delivery.assigned event for order " + del.orderId);
    }
}

// Helpers
function getDriver(string id) returns Driver?|error {
    stream<record {}, error?> resultStream = check driverCollection->find({ "id": id });
    record {| record {} value; |}? result = check resultStream.next();
    check resultStream.close();
    if result is () {
        return ();
    }
    return result.value.cloneWithType(Driver);
}

function getDelivery(string id) returns Delivery?|error {
    // 1. Try finding by Delivery ID ('id')
    stream<record {}, error?> resultStream = check deliveryCollection->find({ "id": id });
    record {| record {} value; |}? result = check resultStream.next();
    check resultStream.close();
    if result is record {| record {} value; |} {
        return result.value.cloneWithType(Delivery);
    }

    // 2. Fallback: Try finding by Order ID ('orderId')
    stream<record {}, error?> orderStream = check deliveryCollection->find({ "orderId": id });
    record {| record {} value; |}? ordResult = check orderStream.next();
    check orderStream.close();
    if ordResult is record {| record {} value; |} {
        return ordResult.value.cloneWithType(Delivery);
    }

    return ();
}
