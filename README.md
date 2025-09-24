# 📦 Shipping Orchestrator Consumer 🚚

Este proyecto forma parte del **bootcamp de Java en NTT Data**.

Este microservicio es responsable de **consumir eventos de despacho** desde Kafka, procesarlos y guardarlos en MongoDB. También puede leer snapshots desde Redis y enviar mensajes a un Dead Letter Topic (DLT) si hay errores.

Se comunica con el microservicio [**Producer**](https://github.com/AlexisJoselyn/shipping-ops-producer) que publica los eventos de envío (ShippingRequestEvent) en Kafka.

---

## Tecnologías

* Java 17
* Spring Boot
* Spring Data Reactive MongoDB
* Spring Data Redis (reactive)
* Apache Kafka
* RxJava 3
* Micrometer (métricas)
* Docker

---

## Configuración y Levantamiento

### Puertos

* Este microservicio corre por defecto en el puerto **8088** (puedes cambiarlo en `application.yml` si quieres).

### Variables de configuración

En `application.yml` o en variables de entorno:

* `app.kafka.bootstrap-servers`: dirección de Kafka
* `app.kafka.group-id`: ID del consumer group
* `app.kafka.schema-registry-url`: URL del Schema Registry
* `app.topics.main`: topic principal de eventos
* `app.topics.dlt`: topic DLT
* `spring.data.mongodb.uri`: URI de MongoDB
* `spring.redis.host` y `spring.redis.port`: configuración de Redis

### Paso a paso para levantar

1. Clonar este repositorio junto al repositorio **Producer** en la **misma carpeta**, para que Docker compose pueda levantar ambos servicios juntos.
2. Revisar `application.yml`
3. Construir con Maven:

```bash
mvn clean package
```

4. Levantar localmente con Docker Compose (asegurarse de que Producer, Consumer, Kafka, MongoDB y Redis estén definidos):

```bash
docker-compose up --build
```

5. Verificar que ambos servicios estén corriendo:

    * Producer: 8087
    * Consumer: 8088

---

## Flujo de comunicación

1. El **Producer** publica un evento `ShippingRequestEvent` en Kafka (topic principal).
2. El **Consumer** lo recibe mediante `KafkaRxConsumer`, lo convierte a modelo de dominio (`ShipmentEvent`) y lo procesa.
3. Dependiendo del intento (`attemptNumber`):

    * 1er intento: guarda en MongoDB y despacha.
    * 2do intento: combina snapshot de Redis si existe, hace upsert en MongoDB y despacha.
4. Si hay errores: el evento se envía al DLT y se hace ack para no bloquear el flujo.

---

## Métricas

Se usan contadores y timers de Micrometer para monitorear:

* Eventos consumidos
* Duplicados
* Primer persist
* Upsert de segundo intento
* Envíos a DLT
* Errores de procesamiento
* Tiempo de procesamiento por evento

---

## Lo que he aprendido

* Cómo consumir eventos con Kafka usando `Reactor Kafka` y `RxJava 3`
* Cómo mapear Avro a modelos de dominio
* Uso de MongoDB reactivo y Redis para snapshots
* Manejo de métricas con Micrometer
* Buenas prácticas de procesamiento de eventos (ack, DLT, retries)
* Cómo organizar un microservicio backend siendo principiante en Java y Spring Boot
