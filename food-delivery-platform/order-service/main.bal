import ballerina/http;
import ballerinax/mongodb;
import ballerinax/kafka;
import ballerina/log;
import ballerina/time;
import ballerina/random;

configurable string mongoHost = "localhost";
configurable int mongoPort = 27017;
configurable string dbName = "order_db";
configurable string kafkaBootstrap = "localhost:9092";

configurable string customerServiceUrl = "http://localhost:9001/customers";
configurable string restaurantServiceUrl = "http://localhost:9002/restaurants";

public type OrderItem record {|
    string itemId;
    int quantity;
    decimal price;
|};

public type Order record {
    string id;
    string customerId;
    string restaurantId;
    string deliveryId = "";
    OrderItem[] items;
    decimal totalPrice;
    string status; // CREATED, CONFIRMED, PREPARING, READY, OUT_FOR_DELIVERY, DELIVERED, CANCELLED
    string createdAt;
    string updatedAt;
};

public type OrderPayload record {|
    string customerId;
    string restaurantId;
    record {| string itemId; int quantity; |}[] items;
|};

// Schema records for validation (open records to allow extra fields like address, email)
type Customer record {
    string id;
    string name;
    decimal balance;
};

type MenuItem record {
    string id;
    string name;
    decimal price;
};

type Restaurant record {
    string id;
    string name;
    MenuItem[] menu;
    map<int> inventory;
    boolean isOpen;
};

// Kafka events structure
type OrderCreatedEvent record {
    string orderId;
    string customerId;
    string restaurantId;
    string deliveryId;
    decimal amount;
};

type PaymentCompletedEvent record {
    string orderId;
    string customerId;
    decimal amount;
    string status; // SUCCESS or FAILED
    string reason;
};

type OrderConfirmedEvent record {
    string orderId;
    string restaurantId;
    string deliveryId;
    decimal amount;
    record {| string itemId; int quantity; |}[] items;
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
final mongodb:Collection orderCollection;
final kafka:Producer kafkaProducer;

final http:Client customerClient;
final http:Client restaurantClient;

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
    orderCollection = check db->getCollection("orders");

    kafkaProducer = check new (kafkaBootstrap, {
        clientId: "order-service-producer"
    });

    customerClient = check new (customerServiceUrl);
    restaurantClient = check new (restaurantServiceUrl);

    log:printInfo("Order Service initialized. Connects to " + customerServiceUrl + " and " + restaurantServiceUrl);
}

service /orders on new http:Listener(9003) {

    // POST /orders - Place an order
    resource function post .(OrderPayload payload) returns Order|http:BadRequest|http:NotFound|http:InternalServerError {
        log:printInfo(string `Received order request for customer ${payload.customerId} at restaurant ${payload.restaurantId}`);
        do {
            // 1. Validate customer exists & has balance
            http:Response custRes = check customerClient->get("/" + payload.customerId);
            if custRes.statusCode == http:STATUS_NOT_FOUND {
                log:printWarn("Customer not found: " + payload.customerId);
                return http:NOT_FOUND;
            } else if custRes.statusCode != http:STATUS_OK {
                return http:INTERNAL_SERVER_ERROR;
            }
            json custJson = check custRes.getJsonPayload();
            Customer customer = check custJson.cloneWithType(Customer);

            // 2. Validate restaurant exists & is open
            http:Response restRes = check restaurantClient->get("/" + payload.restaurantId);
            if restRes.statusCode == http:STATUS_NOT_FOUND {
                log:printWarn("Restaurant not found: " + payload.restaurantId);
                return http:NOT_FOUND;
            } else if restRes.statusCode != http:STATUS_OK {
                return http:INTERNAL_SERVER_ERROR;
            }
            json restJson = check restRes.getJsonPayload();
            Restaurant restaurant = check restJson.cloneWithType(Restaurant);

            if !restaurant.isOpen {
                log:printWarn("Restaurant is closed: " + payload.restaurantId);
                return http:BAD_REQUEST;
            }

            // 3. Verify stock and calculate total price
            decimal totalAmount = 0.0;
            OrderItem[] orderItems = [];

            foreach var orderItem in payload.items {
                // Find item in menu
                MenuItem? menuItem = ();
                foreach var mItem in restaurant.menu {
                    if mItem.id == orderItem.itemId {
                        menuItem = mItem;
                        break;
                    }
                }

                if menuItem is () {
                    log:printWarn(string `Menu item ${orderItem.itemId} not found in restaurant menu`);
                    return http:BAD_REQUEST;
                }

                // Check stock
                int stock = restaurant.inventory.hasKey(orderItem.itemId) ? restaurant.inventory.get(orderItem.itemId) : 0;
                if stock < orderItem.quantity {
                    log:printWarn(string `Insufficient stock for item ${orderItem.itemId}: stock=${stock}, required=${orderItem.quantity}`);
                    return http:BAD_REQUEST;
                }

                decimal itemTotal = menuItem.price * <decimal>orderItem.quantity;
                totalAmount += itemTotal;

                orderItems.push({
                    itemId: orderItem.itemId,
                    quantity: orderItem.quantity,
                    price: menuItem.price
                });
            }

            // 4. Validate customer has enough balance
            if customer.balance < totalAmount {
                log:printWarn(string `Insufficient customer balance: balance=${customer.balance}, total=${totalAmount}`);
                return http:BAD_REQUEST;
            }

            // 5. Create Order with Auto-generated Order ID and Delivery ID
            string orderId = "order_" + (check random:createDecimal()).toString().substring(2, 10);
            string deliveryId = "del_" + (check random:createDecimal()).toString().substring(2, 10);
            string timeNow = time:utcToString(time:utcNow());
            Order newOrder = {
                id: orderId,
                customerId: payload.customerId,
                restaurantId: payload.restaurantId,
                deliveryId: deliveryId,
                items: orderItems,
                totalPrice: totalAmount,
                status: "CREATED",
                createdAt: timeNow,
                updatedAt: timeNow
            };

            check orderCollection->insertOne(newOrder);
            log:printInfo(string `Created order in database: ${orderId} with pre-assigned delivery ID: ${deliveryId}`);

            // 6. Publish orders.created event
            OrderCreatedEvent createdEvent = {
                orderId: orderId,
                customerId: payload.customerId,
                restaurantId: payload.restaurantId,
                deliveryId: deliveryId,
                amount: totalAmount
            };
            json eventJson = createdEvent.toJson();
            check kafkaProducer->send({
                topic: "orders.created",
                value: eventJson.toString().toBytes()
            });
            log:printInfo("Published orders.created event for " + orderId);

            return newOrder;

        } on fail error err {
            log:printError("Error placing order: " + err.message());
            return http:INTERNAL_SERVER_ERROR;
        }
    }

    // GET /orders/[id] - Get order by ID
    resource function get [string id]() returns Order|http:NotFound|http:InternalServerError {
        log:printInfo("Fetching order: " + id);
        do {
            Order? ord = check getOrder(id);
            if ord is Order {
                return ord;
            }
            return http:NOT_FOUND;
        } on fail error err {
            log:printError("Error fetching order: " + err.message());
            return http:INTERNAL_SERVER_ERROR;
        }
    }

    // GET /orders/customer/[customerId] - Get order history for a customer
    resource function get customer/[string customerId]() returns Order[]|http:InternalServerError {
        log:printInfo("Fetching order history for customer: " + customerId);
        do {
            stream<record {}, error?> resultStream = check orderCollection->find({ "customerId": customerId });
            record {| record {} value; |}? result = check resultStream.next();
            Order[] customerOrders = [];
            while result is record {| record {} value; |} {
                Order ord = check result.value.cloneWithType(Order);
                customerOrders.push(ord);
                result = check resultStream.next();
            }
            check resultStream.close();
            return customerOrders;
        } on fail error err {
            log:printError("Error fetching customer orders: " + err.message());
            return http:INTERNAL_SERVER_ERROR;
        }
    }
}

// Kafka Listener to consume lifecycle update events
listener kafka:Listener lifecycleListener = new (kafkaBootstrap, {
    groupId: "order-service-group",
    topics: ["payments.completed", "delivery.assigned", "delivery.completed"]
});

service on lifecycleListener {
    remote function onConsumerRecord(kafka:BytesConsumerRecord[] records) returns error? {
        foreach var rec in records {
            string payloadStr = check string:fromBytes(rec.value);
            log:printInfo("Order Service received event: " + payloadStr);
            json payloadJson = check payloadStr.fromJsonString();
            string topic = rec.offset.partition.topic;

            if topic == "payments.completed" {
                PaymentCompletedEvent event = check payloadJson.cloneWithType(PaymentCompletedEvent);
                check handlePaymentCompleted(event);
            } else if topic == "delivery.assigned" {
                DeliveryAssignedEvent event = check payloadJson.cloneWithType(DeliveryAssignedEvent);
                check updateOrderStatus(event.orderId, "OUT_FOR_DELIVERY");
            } else if topic == "delivery.completed" {
                DeliveryCompletedEvent event = check payloadJson.cloneWithType(DeliveryCompletedEvent);
                check updateOrderStatus(event.orderId, "DELIVERED");
            }
        }
    }
}

function handlePaymentCompleted(PaymentCompletedEvent event) returns error? {
    if event.status == "SUCCESS" {
        log:printInfo(string `Payment successful for order ${event.orderId}. Setting status to CONFIRMED.`);
        check updateOrderStatus(event.orderId, "CONFIRMED");
        
        // Fetch order items to publish orders.confirmed event
        Order? ord = check getOrder(event.orderId);
        if ord is Order {
            record {| string itemId; int quantity; |}[] confirmedItems = [];
            foreach var item in ord.items {
                confirmedItems.push({
                    itemId: item.itemId,
                    quantity: item.quantity
                });
            }
            OrderConfirmedEvent confirmedEvent = {
                orderId: event.orderId,
                restaurantId: ord.restaurantId,
                deliveryId: ord.deliveryId,
                amount: ord.totalPrice,
                items: confirmedItems
            };
            json eventJson = confirmedEvent.toJson();
            check kafkaProducer->send({
                topic: "orders.confirmed",
                value: eventJson.toString().toBytes()
            });
            log:printInfo("Published orders.confirmed event for " + event.orderId);
        }
    } else {
        log:printWarn(string `Payment failed for order ${event.orderId} (Reason: ${event.reason}). Setting status to CANCELLED.`);
        check updateOrderStatus(event.orderId, "CANCELLED");
    }
}

function updateOrderStatus(string orderId, string status) returns error? {
    string timeNow = time:utcToString(time:utcNow());
    _ = check orderCollection->updateOne(
        { "id": orderId },
        { set: { "status": status, "updatedAt": timeNow } }
    );
    log:printInfo(string `Updated status of order ${orderId} to ${status}`);
}

// Helper to query order by Order ID or Delivery ID
function getOrder(string id) returns Order?|error {
    // 1. Try finding by Order ID ('id')
    stream<record {}, error?> resultStream = check orderCollection->find({ "id": id });
    record {| record {} value; |}? result = check resultStream.next();
    check resultStream.close();
    if result is record {| record {} value; |} {
        return result.value.cloneWithType(Order);
    }

    // 2. Fallback: Try finding by Delivery ID ('deliveryId')
    stream<record {}, error?> deliveryStream = check orderCollection->find({ "deliveryId": id });
    record {| record {} value; |}? delResult = check deliveryStream.next();
    check deliveryStream.close();
    if delResult is record {| record {} value; |} {
        return delResult.value.cloneWithType(Order);
    }

    return ();
}
