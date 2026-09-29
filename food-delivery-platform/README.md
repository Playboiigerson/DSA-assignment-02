# Distributed Food Delivery Platform

This is a distributed, event-driven Food Delivery Platform implemented in **Ballerina Swan Lake (Update 12, 2201.12.9)**, utilizing **Apache Kafka** for event-driven messaging, **MongoDB** for persistence, and **Docker Compose** for microservices orchestration.

---

## 🏗️ System Architecture

The platform is designed around 7 decoupled microservices communicating asynchronously via Kafka topics and persisting state in service-specific MongoDB databases.

```
                  +-----------------------------------+
                  |            Client CLI             |
                  +-----------------+-----------------+
                                    |
            +-----------------------+-----------------------+
            | (HTTP)                | (HTTP)                | (HTTP)
            v                       v                       v
  +------------------+    +------------------+    +------------------+
  | Customer Service |    |  Order Service   |    | Delivery Service |
  |   (Port 9001)    |    |   (Port 9003)    |    |   (Port 9005)    |
  +--------+---------+    +--------+---------+    +--------+---------+
           |                       |                       |
           v                       v                       v
     [customer_db]            [order_db]             [delivery_db]
           ^                       |                       ^
           | (HTTP)                |                       | (Assign Driver)
           |                       v                       |
           |               ===============                 |
           |               | Kafka Broker |                |
           |               ===============                 |
           |                       |                       |
           |                       | (orders.ready)        |
           |                       v                       |
  +--------+---------+    +--------+---------+             |
  | Payment Service  |    |Restaurant Service|             |
  | (Kafka Consumer) |    |   (Port 9002)    |-------------+
  +------------------+    +--------+---------+
                                   |
                                   v
                            [restaurant_db]
```

### Microservices Overview

1. **Customer Service** (Port `9001`): Manages customer registration, profiles, and account balance (deductions and deposits).
2. **Restaurant Service** (Port `9002`): Manages digital menus, inventory levels, and starts food preparation upon order confirmation.
3. **Order Service** (Port `9003`): Coordinates the central order lifecycle state machine:
   `CREATED` -> `CONFIRMED` -> `PREPARING` -> `READY` -> `OUT_FOR_DELIVERY` -> `DELIVERED` (or `CANCELLED`).
4. **Payment Service**: Listens for `orders.created`, processes payment via Customer Service, and emits `payments.completed`.
5. **Delivery Service** (Port `9005`): Manages driver status and coordinates assignments. Automatically assigns available drivers to `READY` orders. Exposes driver completion endpoint.
6. **Notification Service**: Consumes all lifecycle topics and outputs mock real-time user-facing notifications.
7. **Admin Service** (Port `9006`): Aggregates performance data (preparation/delivery times) and generates statistics.

---

## ⚡ Event-Driven Workflow

1. **Order Placement**: Customer places order -> Order Service validates customer/restaurant via HTTP -> saves order as `CREATED` -> emits `orders.created`.
2. **Payment**: Payment Service consumes `orders.created` -> deducts customer balance -> emits `payments.completed` (`SUCCESS`/`FAILED`).
3. **Confirmation**: Order Service consumes `payments.completed` -> updates status to `CONFIRMED` -> emits `orders.confirmed`.
4. **Preparation**: Restaurant Service consumes `orders.confirmed` -> updates inventory -> starts food preparation -> emits `orders.preparing` -> after simulation delay, food is ready -> emits `orders.ready`.
5. **Driver Assignment**: Delivery Service consumes `orders.ready` -> creates delivery record -> assigns available driver (`AVAILABLE` -> `BUSY`) -> emits `delivery.assigned`.
6. **Dispatch**: Order Service consumes `delivery.assigned` -> updates status to `OUT_FOR_DELIVERY`.
7. **Completion**: Driver marks delivery completed -> Delivery Service frees driver (`BUSY` -> `AVAILABLE`) -> emits `delivery.completed`.
8. **Finalization**: Order Service consumes `delivery.completed` -> updates status to `DELIVERED`. Notification Service alerts customer. Admin Service compiles report.

---

## 🚀 How to Run the Platform

### Option A: Run via Docker Compose (Recommended)

1. Make sure **Docker Desktop** is running on your machine.
2. In the folder `food-delivery-platform/`, start the cluster:
   ```bash
   docker-compose up --build -d
   ```
3. To interact with the system, run the Client CLI locally:
   ```bash
   cd client-cli
   bal run
   ```

### Option B: Run Services Locally (Development Setup)

If you have MongoDB and Kafka running locally on their default ports:
1. Run each service package using Ballerina:
   ```bash
   # From the respective package directory (e.g. customer-service)
   bal run
   ```
2. Start the CLI console:
   ```bash
   cd client-cli
   bal run
   ```

---

## 📊 Database Schemas

- **customer_db.customers**: `id` (PK), `name`, `email`, `address`, `balance`
- **restaurant_db.restaurants**: `id` (PK), `name`, `address`, `menu` (list of `id`, `name`, `price`), `inventory` (itemId -> stock), `isOpen`
- **order_db.orders**: `id` (PK), `customerId`, `restaurantId`, `items` (list of `itemId`, `quantity`, `price`), `totalPrice`, `status`, `createdAt`, `updatedAt`
- **delivery_db.drivers**: `id` (PK), `name`, `status`
- **delivery_db.deliveries**: `id` (PK), `orderId`, `driverId`, `status`, `assignedAt`, `completedAt`
- **admin_db.restaurant_reports**: `id` (PK), `totalOrders`, `totalRevenue`, `totalPrepTimeSeconds`, `avgPrepTimeSeconds`
- **admin_db.delivery_reports**: `id` ("global"), `totalDeliveries`, `totalDeliveryTimeSeconds`, `avgDeliveryTimeSeconds`
