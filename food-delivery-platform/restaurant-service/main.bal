import ballerina/http;
import ballerinax/mongodb;
import ballerinax/kafka;
import ballerina/log;
import ballerina/lang.runtime;

configurable string mongoHost = "localhost";
configurable int mongoPort = 27017;
configurable string dbName = "restaurant_db";
configurable string kafkaBootstrap = "localhost:9092";

configurable string orderServiceUrl = "http://localhost:9003/orders";

public type MenuItem record {|
    string id;
    string name;
    decimal price;
|};

public type Restaurant record {|
    string id;
    string name;
    string address;
    MenuItem[] menu;
    map<int> inventory; // menuItemId -> stock count
    boolean isOpen;
|};

public type InventoryPayload record {|
    map<int> items; // itemId -> additional stock count
|};

public type OrderConfirmedEvent record {
    string orderId;
    string restaurantId;
    string deliveryId = "";
    record {| string itemId; int quantity; |}[] items;
};

public type OrderPreparingEvent record {|
    string orderId;
    string restaurantId;
    string estimatedTime;
|};

public type OrderReadyEvent record {
    string orderId;
    string restaurantId;
    string deliveryId = "";
};

final mongodb:Client mongoClient;
final mongodb:Collection restaurantCollection;
final kafka:Producer kafkaProducer;
final http:Client orderClient;

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
    restaurantCollection = check db->getCollection("restaurants");

    kafkaProducer = check new (kafkaBootstrap, {
        clientId: "restaurant-service-producer"
    });
    orderClient = check new (orderServiceUrl);
    log:printInfo("Restaurant Service initialized. Connected to MongoDB, Kafka, and Order Service at " + orderServiceUrl);
}

service /restaurants on new http:Listener(9002) {

    // POST /restaurants - Register a new restaurant
    resource function post .(Restaurant newRestaurant) returns Restaurant|http:Conflict|http:InternalServerError {
        log:printInfo("Registering new restaurant: " + newRestaurant.name);
        do {
            string restId = newRestaurant.id.trim();
            if restId == "" {
                restId = check generateNextRestaurantId();
            } else {
                Restaurant? existing = check getRestaurant(restId);
                if existing is Restaurant {
                    return http:CONFLICT;
                }
            }

            Restaurant toSave = {
                id: restId,
                name: newRestaurant.name,
                address: newRestaurant.address,
                menu: newRestaurant.menu,
                inventory: newRestaurant.inventory,
                isOpen: newRestaurant.isOpen
            };

            check restaurantCollection->insertOne(toSave);
            log:printInfo("Restaurant registered successfully with ID: " + restId);
            return toSave;
        } on fail error err {
            log:printError("Error registering restaurant: " + err.message());
            return http:INTERNAL_SERVER_ERROR;
        }
    }

    // GET /restaurants - Get all registered restaurants
    resource function get .() returns Restaurant[]|http:InternalServerError {
        log:printInfo("Fetching all registered restaurants");
        do {
            stream<record {}, error?> resultStream = check restaurantCollection->find({});
            Restaurant[] list = [];
            check resultStream.forEach(function(record {} doc) {
                do {
                    Restaurant rest = check doc.cloneWithType(Restaurant);
                    list.push(rest);
                } on fail error e {
                    log:printError("Error parsing restaurant: " + e.message());
                }
            });
            return list;
        } on fail error err {
            log:printError("Error fetching restaurants: " + err.message());
            return http:INTERNAL_SERVER_ERROR;
        }
    }

    // GET /restaurants/[id] - Get restaurant info
    resource function get [string id]() returns Restaurant|http:NotFound|http:InternalServerError {
        log:printInfo("Fetching restaurant profile: " + id);
        do {
            Restaurant? rest = check getRestaurant(id);
            if rest is Restaurant {
                return rest;
            }
            return http:NOT_FOUND;
        } on fail error err {
            log:printError("Error fetching restaurant: " + err.message());
            return http:INTERNAL_SERVER_ERROR;
        }
    }

    // POST /restaurants/[id]/menu - Add menu items
    resource function post [string id]/menu(MenuItem[] items) returns http:Ok|http:NotFound|http:InternalServerError {
        log:printInfo(string `Adding menu items to restaurant ${id}`);
        do {
            Restaurant? rest = check getRestaurant(id);
            if rest is () {
                return http:NOT_FOUND;
            }
            MenuItem[] currentMenu = rest.menu;
            foreach MenuItem item in items {
                currentMenu.push(item);
                // Also initialize stock in inventory if not present
                if !rest.inventory.hasKey(item.id) {
                    rest.inventory[item.id] = 0;
                }
            }
            _ = check restaurantCollection->updateOne({ "id": id }, { set: { "menu": currentMenu, "inventory": rest.inventory } });
            return http:OK;
        } on fail error err {
            log:printError("Error adding menu items: " + err.message());
            return http:INTERNAL_SERVER_ERROR;
        }
    }

    // POST /restaurants/[id]/inventory - Update inventory (add stock)
    resource function post [string id]/inventory(InventoryPayload payload) returns http:Ok|http:NotFound|http:InternalServerError {
        log:printInfo(string `Updating inventory for restaurant ${id}`);
        do {
            Restaurant? rest = check getRestaurant(id);
            if rest is () {
                return http:NOT_FOUND;
            }
            foreach var [itemId, qty] in payload.items.entries() {
                int currentQty = rest.inventory.hasKey(itemId) ? rest.inventory.get(itemId) : 0;
                rest.inventory[itemId] = currentQty + qty;
            }
            _ = check restaurantCollection->updateOne({ "id": id }, { set: { "inventory": rest.inventory } });
            log:printInfo(string `Successfully updated inventory for restaurant ${id}`);
            return http:OK;
        } on fail error err {
            log:printError("Error updating inventory: " + err.message());
            return http:INTERNAL_SERVER_ERROR;
        }
    }

    // POST /restaurants/[id]/restock-items - Restock inventory items (e.g., on order cancellation)
    resource function post [string id]/restock\-items(record {| record {| string itemId; int quantity; |}[] items; |} payload) returns http:Ok|http:NotFound|http:InternalServerError {
        log:printInfo(string `Restocking items for restaurant ${id}`);
        do {
            Restaurant? rest = check getRestaurant(id);
            if rest is () {
                return http:NOT_FOUND;
            }
            map<int> currentInv = rest.inventory;
            foreach var item in payload.items {
                int stock = currentInv.hasKey(item.itemId) ? currentInv.get(item.itemId) : 0;
                currentInv[item.itemId] = stock + item.quantity;
            }
            _ = check restaurantCollection->updateOne({ "id": id }, { set: { "inventory": currentInv } });
            return http:OK;
        } on fail error err {
            log:printError("Error restocking items: " + err.message());
            return http:INTERNAL_SERVER_ERROR;
        }
    }

    // GET /restaurants/[id]/orders - View active orders for a restaurant
    resource function get [string id]/orders() returns json|http:InternalServerError {
        do {
            http:Response res = check orderClient->get("/restaurant/" + id);
            return check res.getJsonPayload();
        } on fail error err {
            log:printError("Error fetching restaurant orders: " + err.message());
            return http:INTERNAL_SERVER_ERROR;
        }
    }

    // POST /restaurants/[id]/orders/[orderId]/status - Update order status (PREPARING, READY, OUT_FOR_DELIVERY)
    resource function post [string id]/orders/[string orderId]/status(record {| string status; |} payload) returns http:Ok|http:BadRequest|http:NotFound|http:InternalServerError {
        do {
            http:Response res = check orderClient->post("/" + orderId + "/status", payload);
            if res.statusCode == http:STATUS_OK {
                return http:OK;
            } else if res.statusCode == http:STATUS_NOT_FOUND {
                return http:NOT_FOUND;
            } else {
                return http:BAD_REQUEST;
            }
        } on fail error err {
            log:printError("Error updating order status via restaurant service: " + err.message());
            return http:INTERNAL_SERVER_ERROR;
        }
    }
}

// Helper to generate sequential Restaurant ID: rest_01, rest_02, ...
function generateNextRestaurantId() returns string|error {
    stream<record {}, error?> resultStream = check restaurantCollection->find({});
    int count = 0;
    check resultStream.forEach(function(record {} doc) {
        count += 1;
    });

    int nextNum = count + 1;
    while true {
        string formattedNum = nextNum < 10 ? "0" + nextNum.toString() : nextNum.toString();
        string candidateId = "rest_" + formattedNum;
        Restaurant? existing = check getRestaurant(candidateId);
        if existing is () {
            return candidateId;
        }
        nextNum += 1;
    }
}

// Kafka Listener to consume 'orders.confirmed'
listener kafka:Listener confirmedListener = new (kafkaBootstrap, {
    groupId: "restaurant-service-group",
    topics: ["orders.confirmed"]
});

service on confirmedListener {
    remote function onConsumerRecord(kafka:BytesConsumerRecord[] records) returns error? {
        foreach var rec in records {
            string payloadStr = check string:fromBytes(rec.value);
            log:printInfo("Restaurant Service received orders.confirmed event: " + payloadStr);
            
            json payloadJson = check payloadStr.fromJsonString();
            OrderConfirmedEvent event = check payloadJson.cloneWithType(OrderConfirmedEvent);
            
            // Process order preparation asynchronously
            _ = start processOrderPreparation(event);
        }
    }
}

function processOrderPreparation(OrderConfirmedEvent event) returns error? {
    log:printInfo(string `Preparing order ${event.orderId} for restaurant ${event.restaurantId}`);
    
    // 1. Deduct inventory
    do {
        Restaurant? rest = check getRestaurant(event.restaurantId);
        if rest is Restaurant {
            map<int> updatedInv = rest.inventory;
            foreach var item in event.items {
                int currentStock = updatedInv.hasKey(item.itemId) ? updatedInv.get(item.itemId) : 0;
                updatedInv[item.itemId] = int:max(0, currentStock - item.quantity);
            }
            _ = check restaurantCollection->updateOne({ "id": event.restaurantId }, { set: { "inventory": updatedInv } });
            log:printInfo(string `Deducted stock for order ${event.orderId}`);
        }
        
        // 2. Publish 'orders.preparing' event
        OrderPreparingEvent prepEvent = {
            orderId: event.orderId,
            restaurantId: event.restaurantId,
            estimatedTime: "15 minutes"
        };
        json prepJson = prepEvent.toJson();
        check kafkaProducer->send({
            topic: "orders.preparing",
            value: prepJson.toString().toBytes()
        });
        log:printInfo(string `Published orders.preparing event for order ${event.orderId}`);

        // 3. Simulate preparation time (sleep for 5 seconds)
        runtime:sleep(5.0);

        // 4. Publish 'orders.ready' event
        OrderReadyEvent readyEvent = {
            orderId: event.orderId,
            restaurantId: event.restaurantId,
            deliveryId: event.deliveryId
        };
        json readyJson = readyEvent.toJson();
        check kafkaProducer->send({
            topic: "orders.ready",
            value: readyJson.toString().toBytes()
        });
        log:printInfo(string `Published orders.ready event for order ${event.orderId}`);

    } on fail error err {
        log:printError(string `Error processing order preparation for order ${event.orderId}: ${err.message()}`);
    }
}

// Helper to query restaurant
function getRestaurant(string id) returns Restaurant?|error {
    stream<record {}, error?> resultStream = check restaurantCollection->find({ "id": id });
    record {| record {} value; |}? result = check resultStream.next();
    check resultStream.close();
    if result is () {
        return ();
    }
    return result.value.cloneWithType(Restaurant);
}
