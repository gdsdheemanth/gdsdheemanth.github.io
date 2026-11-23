Learn microservices after you can build a good monolith. Interviewers test whether you understand the trade-offs, not just the buzzwords.

This track lists every topic to learn, in order, with mini projects to build and the best resources. Work through the topics with the resources below, then build at least two of the projects.

## Concepts & design

### Monolith vs modular monolith vs microservices: pros, cons, when not to use microservices

Learn this from the resources at the end of the section.

### Domain-Driven Design basics: bounded contexts, aggregates, ubiquitous language

Learn this from the resources at the end of the section.

### Service decomposition strategies (by business capability / subdomain)

Learn this from the resources at the end of the section.

### Database per service; shared-database anti-pattern

Learn this from the resources at the end of the section.

### Twelve-Factor App principles

Learn this from the resources at the end of the section.


## Communication

### Synchronous: REST, gRPC (awareness); asynchronous: messaging/events

Learn this from the resources at the end of the section.

### API Gateway (Spring Cloud Gateway): routing, auth, rate limiting

Learn this from the resources at the end of the section.

### Service discovery (Eureka or Kubernetes DNS), client-side vs server-side load balancing

Learn this from the resources at the end of the section.

### OpenFeign / RestClient for service-to-service calls

Learn this from the resources at the end of the section.

### API contracts, versioning, backward compatibility

Learn this from the resources at the end of the section.


## Resilience

### Timeouts, retries with exponential backoff & jitter

Learn this from the resources at the end of the section.

### Circuit breaker, bulkhead, rate limiter, fallbacks (Resilience4j)

Learn this from the resources at the end of the section.

### Idempotency keys & idempotent consumers

Learn this from the resources at the end of the section.

### Cascading failures and how to prevent them

Learn this from the resources at the end of the section.


## Data consistency patterns

### Why distributed transactions / 2PC are avoided; eventual consistency

Learn this from the resources at the end of the section.

### Saga pattern: choreography vs orchestration, compensating transactions

Learn this from the resources at the end of the section.

### Transactional outbox pattern (+ CDC with Debezium)

Learn this from the resources at the end of the section.

### CQRS and event sourcing: concepts

Learn this from the resources at the end of the section.


## Operations & observability

### Centralized config (Spring Cloud Config / K8s ConfigMaps), secrets management

Learn this from the resources at the end of the section.

### Centralized logging (ELK/Loki), correlation IDs

Learn this from the resources at the end of the section.

### Metrics with Micrometer + Prometheus + Grafana

Learn this from the resources at the end of the section.

### Distributed tracing with OpenTelemetry (Zipkin/Jaeger/Tempo)

Learn this from the resources at the end of the section.

### Health checks, readiness/liveness probes

Learn this from the resources at the end of the section.

### Service-to-service security: OAuth2 client credentials, mTLS awareness

Learn this from the resources at the end of the section.

### Deployment strategies: rolling, blue-green, canary

Learn this from the resources at the end of the section.

## Hands-on mini projects

Build at least two of these before moving on. Put each on GitHub with a short README.

### Split the Blog API into services (Intermediate)

Break your monolith into user, post and comment services + API gateway + service discovery + config server. Document the before and after.

**Practises:** Decomposition, Spring Cloud Gateway, Eureka, Config

### Resilience Lab (Intermediate)

Make one service slow/failing on purpose; add timeouts, retries, a circuit breaker and fallbacks with Resilience4j; show the behaviour in logs/metrics.

**Practises:** Resilience4j, failure handling

### Observability Stack (Advanced)

Add Micrometer metrics, Prometheus, Grafana dashboards, OpenTelemetry tracing and correlation IDs to your services; trace one request end to end.

**Practises:** Metrics, tracing, logging

### Saga demo: Travel booking (Advanced)

Book flight + hotel + car as separate services; if one fails, compensate the others. Build it once with choreography and once with an orchestrator.

**Practises:** Saga, compensating transactions, eventual consistency

## Resources

::: resources
- [microservices.io, Chris Richardson](https://microservices.io/patterns/index.html) · Pattern language: saga, outbox, CQRS…
- [Martin Fowler: Microservices](https://martinfowler.com/microservices/) · Foundational articles
- [Spring Cloud](https://spring.io/projects/spring-cloud) · Gateway, Config, OpenFeign, Circuit Breaker
- [Resilience4j docs](https://resilience4j.readme.io/docs) · Circuit breaker, retry, rate limiter
- [The Twelve-Factor App](https://12factor.net/) · Principles for cloud-native services
- [OpenTelemetry docs](https://opentelemetry.io/docs/) · Distributed tracing standard
- Video: [Amigoscode: Microservices](https://www.youtube.com/results?search_query=Amigoscode+microservices+spring+boot) · Build microservices with Spring
- Video: [ByteByteGo](https://www.youtube.com/results?search_query=ByteByteGo+microservices) · Visual architecture explainers
- Book: [Building Microservices (2nd ed.), Sam Newman](https://samnewman.io/books/building_microservices_2nd_edition/) · The standard book
- Book: [Microservices Patterns, Chris Richardson](https://www.manning.com/books/microservices-patterns) · Patterns in Java, in depth
:::
