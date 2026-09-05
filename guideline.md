# Guideline de Proyecto: Sistema Distribuido Event-Driven con Java Spring Boot (E-Commerce Order Engine)

Este documento define la guía técnica, arquitectura y patrones de desarrollo para construir un sistema de microservicios distribuido basado en eventos utilizando **Java 21** y **Spring Boot 3**. Diseñado para destacar en un perfil **Mid-Senior**.

---

## 1. Visión General del Proyecto

Un sistema de **Procesamiento de Pedidos y Notificaciones en Tiempo Real**.
La aplicación recibe la orden a través de un endpoint HTTP REST, responde inmediatamente al cliente con un estado `PENDING` (`202 Accepted`) y procesa el flujo completo (reserva de inventario, pago simulado y notificación) de forma asíncrona a través de un Message Broker.

---

## 2. Stack Tecnológico

### Backend Services (Java Ecosystem)
* **Lenguaje & JDK:** Java 21 (Aprovechando Virtual Threads / Project Loom).
* **Framework:** Spring Boot 3.2+.
* **Ecosistema Spring:**
  * `Spring Boot Starter Web` (APIs REST).
  * `Spring Data JPA` + Hibernate (Persistencia relacional).
  * `Spring Kafka` / `Spring Cloud Stream` (Abstracción de eventos).
  * `Resilience4j` (Circuit Breakers, Retries, Rate Limiters).
  * `Lombok` & `MapStruct` (Reducción de boilerplate y mapeo de DTOs).
* **Bases de Datos:**
  * `Order Service`: PostgreSQL
  * `Inventory Service`: PostgreSQL
  * `Notification Service`: Redis (Caché, persistencia rápida y soporte para WebSockets)
* **Message Broker:** **Apache Kafka** (con Zookeeper/KRaft) o **RabbitMQ**.
* **Build Tool:** Apache Maven (Estructura Multi-Módulo).

### Frontend (Opcional)
* **Framework:** React / Next.js con TailwindCSS.
* **Propósito:** Dashboard en tiempo real conectado vía WebSockets / SSE para monitorear el estado de los pedidos.

### Infraestructura & Observabilidad
* **Docker & Docker Compose:** Contenedorización de microservicios, brokers y bases de datos.
* **Observabilidad:** Micrometer + Prometheus + Grafana (Métricas y Tracing).

---

## 3. Arquitectura y Módulos de la Aplicación

```
[ Frontend / Cliente ]
         │ (HTTP / WebSockets)
         ▼
 ┌───────────────┐        Evento: order.created        ┌─────────────────┐
 │ Order Service │ ──────────────────────────────────► │ Message Broker  │
 └───────────────┘                                     │ (Kafka/Rabbit)  │
         ▲                                             └────────┬────────┘
         │ Evento: order.status_updated                         │
         └──────────────────────────────────────────────────────┴──┐
                                                                   │
                                           ┌───────────────────────┴───────────────────────┐
                                           ▼                                               ▼
                               ┌──────────────────────┐                       ┌─────────────────────────┐
                               │  Inventory Service   │                       │  Notification Service   │
                               └──────────────────────┘                       └─────────────────────────┘
```

### A. Order Service (`order-service`)
* **Endpoints HTTP:**
  * `POST /api/v1/orders` -> Crea la orden y retorna estado `PENDING` (`202 Accepted`).
  * `GET /api/v1/orders/{id}` -> Consulta el estado actual del pedido.
  * `GET /api/v1/orders` -> Lista las órdenes del usuario.
* **Eventos que Publica:**
  * `order.created` -> `{ orderId, userId, items: [{ productId, quantity }], totalAmount }`
* **Eventos que Consume:**
  * `inventory.reserved` -> Actualiza estado a `PROCESSING`.
  * `inventory.failed` -> Actualiza estado a `CANCELLED_NO_STOCK`.
  * `payment.succeeded` -> Actualiza estado a `COMPLETED`.
  * `payment.failed` -> Actualiza estado a `CANCELLED_PAYMENT_FAILED`.

### B. Inventory Service (`inventory-service`)
* **Endpoints HTTP:**
  * `GET /api/v1/products` -> Lista de catálogo y stock.
  * `POST /api/v1/products` -> Creación/Actualización de inventario.
* **Eventos que Consume:**
  * `order.created` -> Verifica y descuenta stock.
* **Eventos que Publica:**
  * `inventory.reserved` -> `{ orderId, items }`
  * `inventory.failed` -> `{ orderId, reason }`

### C. Payment Service (`payment-service`)
* **Eventos que Consume:**
  * `inventory.reserved` -> Simula el cobro con un gateway externo.
* **Eventos que Publica:**
  * `payment.succeeded` -> `{ orderId, transactionId }`
  * `payment.failed` -> `{ orderId, reason }`

### D. Notification Service (`notification-service`)
* **Endpoints (WebSockets / SSE):**
  * `WS /ws/orders` -> Notifica cambios de estado en vivo al cliente/dashboard.
* **Eventos que Consume:**
  * `order.created`, `payment.succeeded`, `payment.failed` -> Guarda log e independiza el envío de emails/push.

---

## 4. Modelado de Datos (Esquema SQL)

### Order Service DB (`orders_db` - PostgreSQL)

```sql
CREATE TYPE order_status AS ENUM (
  'PENDING', 
  'PROCESSING', 
  'COMPLETED', 
  'CANCELLED_NO_STOCK', 
  'CANCELLED_PAYMENT_FAILED'
);

CREATE TABLE orders (
  id UUID PRIMARY KEY,
  user_id VARCHAR(55) NOT NULL,
  status VARCHAR(30) NOT NULL DEFAULT 'PENDING',
  total_amount DECIMAL(10,2) NOT NULL,
  idempotency_key VARCHAR(255) UNIQUE NOT NULL,
  created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
  updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE order_items (
  id UUID PRIMARY KEY,
  order_id UUID NOT NULL REFERENCES orders(id) ON DELETE CASCADE,
  product_id VARCHAR(55) NOT NULL,
  quantity INT NOT NULL,
  unit_price DECIMAL(10,2) NOT NULL
);

-- Tabla para Transactional Outbox Pattern
CREATE TABLE outbox_events (
  id UUID PRIMARY KEY,
  aggregate_type VARCHAR(255) NOT NULL,
  aggregate_id VARCHAR(255) NOT NULL,
  type VARCHAR(255) NOT NULL,
  payload JSONB NOT NULL,
  processed BOOLEAN DEFAULT FALSE,
  created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);
```

### Inventory Service DB (`inventory_db` - PostgreSQL)

```sql
CREATE TABLE products (
  id VARCHAR(55) PRIMARY KEY,
  name VARCHAR(255) NOT NULL,
  stock INT NOT NULL CHECK (stock >= 0),
  price DECIMAL(10,2) NOT NULL,
  updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

-- Tabla para Idempotencia de Consumo
CREATE TABLE processed_events (
  event_id VARCHAR(255) PRIMARY KEY,
  processed_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);
```

---

## 5. Patrones Arquitectónicos e Implementación en Spring

### 1. Transactional Outbox Pattern (`@Transactional` + Scheduled Publisher)
En `Order Service`, para evitar inconsistencias entre la DB y Kafka:

```java
@Service
@RequiredArgsConstructor
public class CreateOrderUseCase {

    private final OrderRepository orderRepository;
    private final OutboxEventRepository outboxRepository;
    private final ObjectMapper objectMapper;

    @Transactional
    public OrderResponseDto execute(CreateOrderCommand command) {
        Order order = Order.create(command.getUserId(), command.getItems());
        orderRepository.save(order);

        OutboxEvent outboxEvent = OutboxEvent.builder()
                .id(UUID.randomUUID())
                .aggregateType("ORDER")
                .aggregateId(order.getId().toString())
                .type("order.created")
                .payload(objectMapper.writeValueAsString(OrderCreatedEvent.from(order)))
                .processed(false)
                .build();

        outboxRepository.save(outboxEvent);
        return OrderMapper.toDto(order);
    }
}
```

### 2. Consumo Idempotente (`@KafkaListener`)

```java
@Component
@RequiredArgsConstructor
@Slf4j
public class OrderCreatedEventListener {

    private final InventoryService inventoryService;
    private final ProcessedEventRepository processedEventRepository;

    @KafkaListener(topics = "order-created-topic", groupId = "inventory-group")
    @Transactional
    public void handleOrderCreated(OrderCreatedEvent event) {
        if (processedEventRepository.existsById(event.getEventId())) {
            log.info("Evento omitido por idempotencia: {}", event.getEventId());
            return;
        }

        inventoryService.reserveStock(event);
        processedEventRepository.save(new ProcessedEvent(event.getEventId(), LocalDateTime.now()));
    }
}
```

---

## 6. Estructura del Repositorio (Maven Multi-Module)

```text
ecommerce-event-driven/
├── pom.xml                        # Parent POM (Gestión centralizada de dependencias)
├── order-service/
│   ├── pom.xml
│   ├── Dockerfile
│   └── src/main/java/com/ecommerce/order/
│       ├── config/                # KafkaConfig, SecurityConfig
│       ├── domain/                # Entidades, Enums, Interfaces
│       ├── infrastructure/        # Repositorios JPA, Consumers, Producers
│       └── web/                   # OrderController, DTOs
├── inventory-service/
│   ├── pom.xml
│   ├── Dockerfile
│   └── src/main/java/com/ecommerce/inventory/
├── payment-service/
├── notification-service/
├── docker-compose.yml             # Infraestructura unificada
├── .env.example
├── README.md
└── guideline.md
```

---

## 7. Publicación y Entrega en GitHub

1. **Compilación y Despliegue en 1 Comando:**
   ```bash
   # Compilar todos los módulos con Maven y levantar contenedores
   mvn clean package -DskipTests && docker compose up --build
   ```
2. **Diagrama de Secuencia Mermaid:** Incluir el diagrama del flujo en el `README.md`:
   ```mermaid
   sequenceDiagram
       autonumber
       Client->>OrderService: POST /api/v1/orders
       OrderService-->>Client: 202 Accepted { orderId, status: PENDING }
       OrderService->>Broker: Publish: order.created
       Broker->>InventoryService: Consume: order.created
       alt Stock Disponible
           InventoryService->>Broker: Publish: inventory.reserved
           Broker->>PaymentService: Consume: inventory.reserved
           PaymentService->>Broker: Publish: payment.succeeded
           Broker->>OrderService: Consume: payment.succeeded (Status -> COMPLETED)
       else Sin Stock
           InventoryService->>Broker: Publish: inventory.failed
           Broker->>OrderService: Consume: inventory.failed (Status -> CANCELLED)
       end
   ```
3. **Colección de Pruebas:** Archivo `e-commerce-spring.postman_collection.json` en `docs/` con las peticiones HTTP preparadas.
