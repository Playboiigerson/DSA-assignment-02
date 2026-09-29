import ballerina/http;
import ballerinax/mongodb;
import ballerina/log;

configurable string mongoHost = "localhost";
configurable int mongoPort = 27017;
configurable string dbName = "customer_db";

public type Customer record {|
    string id;
    string name;
    string email;
    string address;
    decimal balance;
|};

public type BalancePayload record {|
    decimal amount;
|};

final mongodb:Client mongoClient;
final mongodb:Collection customerCollection;

// Initialization function
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
    customerCollection = check db->getCollection("customers");
    log:printInfo("Customer Service initialized. Connected to MongoDB at " + mongoHost + ":" + mongoPort.toString());
}

service /customers on new http:Listener(9001) {

    // POST /customers - Register a new customer
    resource function post .(Customer newCustomer) returns Customer|http:BadRequest|http:Conflict|http:InternalServerError {
        log:printInfo("Registering new customer with email: " + newCustomer.email);
        do {
            // 1. Email format validation
            string email = newCustomer.email.trim();
            if email == "" || !email.includes("@") || !email.includes(".") || email.indexOf("@") < 1 || email.lastIndexOf(".") <= email.indexOf("@") + 1 || email.lastIndexOf(".") == email.length() - 1 {
                log:printWarn("Invalid email format: " + newCustomer.email);
                return http:BAD_REQUEST;
            }

            // 2. Starting balance non-negative validation
            if newCustomer.balance < 0.0d {
                log:printWarn("Starting balance cannot be negative: " + newCustomer.balance.toString());
                return http:BAD_REQUEST;
            }

            // 3. Email uniqueness check
            Customer? existingWithEmail = check getCustomerByEmail(email);
            if existingWithEmail is Customer {
                log:printWarn("Email already exists: " + email);
                return http:CONFLICT;
            }

            // 4. Auto-generate Sequential Customer ID (cust_01, cust_02, etc.) if not provided
            string custId = newCustomer.id.trim();
            if custId == "" {
                custId = check generateNextCustomerId();
            } else {
                // If ID is provided, verify it's unique
                Customer? existingWithId = check getCustomer(custId);
                if existingWithId is Customer {
                    log:printWarn("Customer ID already exists: " + custId);
                    return http:CONFLICT;
                }
            }

            Customer toSave = {
                id: custId,
                name: newCustomer.name,
                email: email,
                address: newCustomer.address,
                balance: newCustomer.balance
            };

            check customerCollection->insertOne(toSave);
            log:printInfo("Customer registered successfully with ID: " + custId);
            return toSave;
        } on fail error err {
            log:printError("Error registering customer: " + err.message());
            return http:INTERNAL_SERVER_ERROR;
        }
    }

    // GET /customers/[id] - Get customer profile
    resource function get [string id]() returns Customer|http:NotFound|http:InternalServerError {
        log:printInfo("Fetching customer profile: " + id);
        do {
            Customer? cust = check getCustomer(id);
            if cust is Customer {
                return cust;
            }
            return http:NOT_FOUND;
        } on fail error err {
            log:printError("Error fetching customer: " + err.message());
            return http:INTERNAL_SERVER_ERROR;
        }
    }

    // POST /customers/[id]/deduct-balance - Deduct from customer balance
    resource function post [string id]/deduct\-balance(BalancePayload payload) returns http:Ok|http:BadRequest|http:NotFound|http:InternalServerError {
        log:printInfo(string `Deducting ${payload.amount} from customer ${id}`);
        do {
            Customer? cust = check getCustomer(id);
            if cust is () {
                return http:NOT_FOUND;
            }
            if cust.balance < payload.amount {
                log:printWarn(string `Insufficient balance for customer ${id}: balance=${cust.balance}, required=${payload.amount}`);
                return http:BAD_REQUEST;
            }
            decimal newBalance = cust.balance - payload.amount;
            _ = check customerCollection->updateOne({ "id": id }, { set: { "balance": newBalance } });
            log:printInfo(string `Successfully deducted ${payload.amount} from customer ${id}. New balance is ${newBalance}`);
            return http:OK;
        } on fail error err {
            log:printError("Error deducting balance: " + err.message());
            return http:INTERNAL_SERVER_ERROR;
        }
    }

    // POST /customers/[id]/add-balance - Add to customer balance
    resource function post [string id]/add\-balance(BalancePayload payload) returns http:Ok|http:NotFound|http:InternalServerError {
        log:printInfo(string `Adding ${payload.amount} to customer ${id}`);
        do {
            Customer? cust = check getCustomer(id);
            if cust is () {
                return http:NOT_FOUND;
            }
            decimal newBalance = cust.balance + payload.amount;
            _ = check customerCollection->updateOne({ "id": id }, { set: { "balance": newBalance } });
            log:printInfo(string `Successfully added ${payload.amount} to customer ${id}. New balance is ${newBalance}`);
            return http:OK;
        } on fail error err {
            log:printError("Error adding balance: " + err.message());
            return http:INTERNAL_SERVER_ERROR;
        }
    }
}

// Helper to generate sequential Customer ID: cust_01, cust_02, ...
function generateNextCustomerId() returns string|error {
    stream<record {}, error?> resultStream = check customerCollection->find({});
    int count = 0;
    check resultStream.forEach(function(record {} doc) {
        count += 1;
    });

    int nextNum = count + 1;
    while true {
        string formattedNum = nextNum < 10 ? "0" + nextNum.toString() : nextNum.toString();
        string candidateId = "cust_" + formattedNum;
        Customer? existing = check getCustomer(candidateId);
        if existing is () {
            return candidateId;
        }
        nextNum += 1;
    }
}

// Helper to query customer by ID
function getCustomer(string id) returns Customer?|error {
    stream<record {}, error?> resultStream = check customerCollection->find({ "id": id });
    record {| record {} value; |}? result = check resultStream.next();
    check resultStream.close();
    if result is () {
        return ();
    }
    return result.value.cloneWithType(Customer);
}

// Helper to query customer by Email
function getCustomerByEmail(string email) returns Customer?|error {
    stream<record {}, error?> resultStream = check customerCollection->find({ "email": email });
    record {| record {} value; |}? result = check resultStream.next();
    check resultStream.close();
    if result is () {
        return ();
    }
    return result.value.cloneWithType(Customer);
}
