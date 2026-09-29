# Replicating the Ballerina REST & gRPC Service Architecture

This document provides a comprehensive, copy-pasteable **AI System Prompt** along with a **Developer Replication Guide** for creating network microservices using the exact stack, design patterns, and programming language from your current assignment.

---

## 📋 Technology Stack Overview

1. **Language & Version**: **Ballerina Swan Lake (Update 12, 2201.12.9)**.
2. **REST API Component**: Built-in `ballerina/http` module supporting resource paths, JSON-to-Record binding (`cloneWithType`), and standard HTTP status code returns (e.g. `201 Created`, `409 Conflict`, `404 Not Found`).
3. **gRPC API Component**: Built-in `ballerina/grpc` module coupled with **Protocol Buffers v3 (`.proto`)** definitions, utilizing `bal grpc` CLI compilation to auto-generate client/server stub files (`*_pb.bal`).
4. **State Management**: Memory-resident relational tables (`table<Record> key(field)`) with query expressions (`from ... where ... select ...`).
5. **Standard Libraries**:
   - `ballerina/time`: Parsers (`time:utcFromString`), UTC timestamps (`time:utcNow`), and time differences (`time:utcDiffSeconds`).
   - `ballerina/io`: Terminal CLI interactive loops.

---

## 🤖 The AI Replication Prompt (Copy-Paste to a new Chat)

*Copy the block below and send it to an AI coding assistant (like Gemini, ChatGPT, or Claude) to jumpstart your next assignment.*

````markdown
I need you to build a system consisting of a client-server microservices architecture written in **Ballerina Swan Lake (Update 12, 2201.12.9)**. 
The system should have two parts or options for communication: 
1. A **REST/HTTP** microservice.
2. A **gRPC** microservice.

Use the following architectural specifications, layout rules, and coding patterns:

### 1. Project Directory Layout
Organize the code into separate Ballerina packages for client and server to keep dependencies isolated:
```
ProjectRoot/
│
├── Question_1/                    # REST / HTTP Service
│   ├── server/
│   │   ├── Ballerina.toml
│   │   └── main.bal               # Server listeners & HTTP resources
│   └── client/
│       ├── Ballerina.toml
│       └── main.bal               # CLI console menu making client HTTP calls
│
└── Question_2/                    # gRPC Service
    ├── service_def.proto          # Protobuf v3 definitions
    ├── server/
    │   ├── Ballerina.toml
    │   ├── main_service.bal       # gRPC remote methods implementation
    │   └── service_pb.bal         # Generated protobuf stubs
    └── client/
        ├── Ballerina.toml
        ├── main_client.bal        # CLI console menu calling gRPC endpoints
        └── service_pb.bal         # Generated protobuf stubs
```

### 2. Ballerina Code Guidelines & Syntax Patterns

#### A. In-Memory Tables for State Management
Use Ballerina's typed `table` data structure to store records. Always define a primary key:
```ballerina
public type Entity record {|
    readonly string id;
    string name;
    string status;
|};

// In-memory data store
public final table<Entity> key(id) entity_table = table [];
```

#### B. HTTP/REST Service Implementation (`ballerina/http`)
- Bind services to standard ports (e.g., `9000`).
- Map JSON request payloads to strongly-typed records using `req.getJsonPayload()` followed by `cloneWithType()`.
- Return proper status codes (`http:STATUS_NOT_FOUND`, `http:STATUS_CONFLICT`, `http:STATUS_CREATED`, `http:STATUS_NO_CONTENT`).
```ballerina
import ballerina/http;

service /entity-manager on new http:Listener(9000) {

    // GET Collection
    resource function get entities() returns Entity[] {
        return entity_table.toArray();
    }

    // POST Resource (Create)
    resource function post entities(http:Caller caller, http:Request req) returns error? {
        var payload = req.getJsonPayload();
        if payload is json {
            Entity newEntity = check payload.cloneWithType(Entity);
            if entity_table.hasKey(newEntity.id) {
                http:Response errRes = new;
                errRes.statusCode = http:STATUS_CONFLICT;
                errRes.setPayload({errmsg: "ID already exists"});
                check caller->respond(errRes);
            } else {
                entity_table.add(newEntity);
                check caller->respond(newEntity);
            }
        }
    }
}
```

#### C. gRPC Service Implementation (`ballerina/grpc`)
Define services in `.proto` files using standard Protocol Buffers v3. 
- Implement **Unary RPCs** (e.g., `rpc Add(Request) returns (Response)`).
- Implement **Client Streaming RPCs** (e.g., `rpc StreamData(stream StreamRequest) returns (StreamResponse)`).
- Use generated descriptors (`@grpc:Descriptor`) to wire the logic to the listener:
```ballerina
import ballerina/grpc;

listener grpc:Listener ep = new (9090);

@grpc:Descriptor {value: MY_SERVICE_DESC}
service "MyService" on ep {
    
    // Unary RPC Implementation
    remote function Add(AddRequest value) returns Response|error {
        // Business logic here
        return {status: "Success"};
    }

    // Client Streaming RPC Implementation
    remote function StreamData(stream<StreamRequest, grpc:Error?> clientStream) returns StreamResponse|error {
        StreamRequest[] items = [];
        error? e = clientStream.forEach(isolated function(StreamRequest item) {
            items.push(item);
        });
        if e is error { return e; }
        return {message: "Successfully processed " + items.length().toString() + " items"};
    }
}
```

#### D. Date and Time Management (`ballerina/time`)
- Parse ISO dates (`yyyy-MM-dd`) by appending UTC suffix: `isoDate + "T00:00:00Z"`.
- Fetch current time with `time:utcNow()`.
- Parse strings using `time:utcFromString()`.
- Compare times or compute date intervals using `time:utcDiffSeconds()`:
```ballerina
import ballerina/time;

string targetDateStr = "2026-06-18T00:00:00Z";
time:Utc targetTime = check time:utcFromString(targetDateStr);
time:Utc currentTime = time:utcNow();

if targetTime < currentTime {
    // Overdue logic
}

time:Seconds diffSeconds = time:utcDiffSeconds(targetTime, currentTime);
decimal days = diffSeconds / (60 * 60 * 24);
```

#### E. Client Console CLI loops (`ballerina/io`)
Implement user menus using a infinite `while` loop, parsing strings into options or numeric conversions via standard libraries:
```ballerina
import ballerina/io;

public function main() returns error? {
    while true {
        io:println("\n=== System Menu ===");
        io:println("1. Add Data");
        io:println("2. Exit");
        string choice = io:readln("Enter choice: ");
        
        match choice {
            "1" => {
                string name = io:readln("Enter name: ");
                // perform call
            }
            "2" => { break; }
        }
    }
}
```

Please design the requested application mirroring these structures and patterns. Create the required record models, server modules, and interactive console clients.
````

---

## 🛠️ Developer Replication & Development Guide

Follow these steps to construct, build, and test a system replicating the provided assignment codebase.

### Step 1: Install Ballerina
Download and install **Ballerina Swan Lake (Update 12)** on your system.
Verify the installation by running:
```powershell
bal version
```

### Step 2: Initialize Projects
Create separate directories for REST and gRPC services. For each microservice component (server and client), initialize a Ballerina package:

```powershell
# Create REST directories
mkdir -p Question_1/server
mkdir -p Question_1/client

# Initialize Ballerina projects
cd Question_1/server
bal new --template service
cd ../client
bal new --template default

# Create gRPC directories
cd ../../
mkdir -p Question_2/server
mkdir -p Question_2/client
cd Question_2/server
bal new --template service
cd ../client
bal new --template default
```

### Step 3: gRPC Protobuf Setup & Code Generation
1. Create a `service_def.proto` file in the parent `Question_2` folder.
2. Compile and generate Ballerina stubs using the `bal grpc` command. Copy the output file (e.g. `service_pb.bal`) into both the server and client packages:

```powershell
# Run code generation from Question_2 directory
bal grpc --input service_def.proto --output server/
bal grpc --input service_def.proto --output client/
```

### Step 4: Run the Services
To build and run packages during development, run the following commands inside the respective project directories:

```powershell
# Running REST Server (e.g., from Question_1/server)
bal run

# Running REST Client (e.g., from Question_1/client)
bal run

# Running gRPC Server (e.g., from Question_2/server)
bal run

# Running gRPC Client (e.g., from Question_2/client)
bal run
```

### Step 5: Packaging for Production
If you need standalone runnable JAR files, build the executable package using:
```powershell
bal build
```
The output executable will be created in the `target/bin/` folder inside each package directory as a `.jar` file, which you can run using standard Java 21+:
```powershell
java -jar target/bin/server.jar
```
