# E-Commerce Event-Driven Engine (Microservices with Java 21 & Spring Boot 3)

Sistema distribuido de procesamiento de órdenes de comercio electrónico basado en eventos (**Event-Driven Architecture**), implementado con **Java 21**, **Spring Boot 3**, **Apache Kafka**, **PostgreSQL** y **Redis**.

---

## Architecture Overview

```
[ Client / Frontend ]
         │ (POST /orders)
         ▼
 ┌───────────────┐        Event: order.created         ┌─────────────────┐
 │ Order Service │ ──────────────────────────────────► │  Apache Kafka   │
 └───────────────┘                                     │ (Message Broker)│
         ▲                                             └────────┬────────┘
         │ Event: payment.succeeded / failed                    │
         └──────────────────────────────────────────────────────┼──┐
                                                                │  │
                                        ┌───────────────────────┴──┼───────────────────┐
                                        ▼                          │                   ▼
                            ┌──────────────────────┐               │       ┌─────────────────────────┐
                            │  Inventory Service   │               │       │  Notification Service   │
                            └──────────┬───────────┘               │       └─────────────────────────┘
                                       │ Event: inventory.reserved │
                                       ▼                           │
                            ┌──────────────────────┐               │
                            │   Payment Service    │ ──────────────┘
                            └──────────────────────┘
```

## Services & Infrastructure

| Service / Component        | Port   | Database / Storage                          | Description                                             |
| :------------------------- | :----- | :------------------------------------------ | :------------------------------------------------------ |
| **`order-service`**        | `8081` | PostgreSQL 16 (`orders_db`, port `5432`)    | Order management, Outbox Pattern publisher              |
| **`inventory-service`**    | `8082` | PostgreSQL 16 (`inventory_db`, port `5433`) | Stock deduction & idempotent consumption                |
| **`payment-service`**      | `8083` | _Stateless_                                 | External payment gateway integration simulation         |
| **`notification-service`** | `8084` | Redis 7 (`6379`)                            | WebSockets / SSE alerts & notification history          |
| **Apache Kafka**           | `9092` | Zookeeper (`2181`)                          | Distributed event streaming broker                      |
| **Kafka UI**               | `8085` | -                                           | Web UI to inspect topics, consumer groups, and messages |

---

## Project Setup (Developer Guide)

This section contains step-by-step instructions for local development and running the environment.

### 1. Prerequisites

Ensure you have the following installed on your machine:

- **Java Development Kit (JDK):** Version `21` (e.g., Eclipse Temurin, GraalVM or Amazon Corretto).
- **Docker & Docker Compose:** Version `2.20+` or Docker Desktop.
- **Apache Maven:** Version `3.9+` (or use the included `./mvnw` wrapper).

---

### 2. Running Full Infrastructure (Recommended for E2E Testing)

To start Kafka, Zookeeper, Kafka UI, PostgreSQL databases, and Redis in the background:

```bash
# From the project root directory:
docker compose up -d
```

#### Access Developer Portals:

- **Kafka UI:** [http://localhost:8085](http://localhost:8085)
- **PostgreSQL (Order Service):** `localhost:5432` (User: `postgres`, Pass: `postgrespassword`, DB: `orders_db`)
- **PostgreSQL (Inventory Service):** `localhost:5433` (User: `postgres`, Pass: `postgrespassword`, DB: `inventory_db`)
- **Redis (Notification Service):** `localhost:6379`

To stop the entire infrastructure:

```bash
docker compose down
```

---

### 3. Isolated Microservice Development (Single Service Mode)

If you are developing a single microservice (e.g., `order-service`) and want to save system resources:

1. **Start only the required service database:**
   ```bash
   cd apps/order-service
   docker compose up -d
   ```
2. **Run the Spring Boot application from your IDE** or terminal:
   ```bash
   # From the specific microservice folder:
   ../../mvnw spring-boot:run
   ```

---

### 4. Build and Test Commands

- **Compile and build all modules:**

  ```bash
  mvn clean install -DskipTests
  ```

- **Run tests across all modules:**
  ```bash
  mvn test
  ```

---

### 5. Future Roadmap & Notes

- [ ] Implement Shared `common-dto` module for domain events.
- [ ] Add Flyway / Liquibase database migrations for PostgreSQL schemas.
- [ ] Add Resilience4j Circuit Breakers & Dead Letter Topic (`@RetryableTopic`) configurations.
- [ ] Add OpenAPI / Swagger UI endpoints.
