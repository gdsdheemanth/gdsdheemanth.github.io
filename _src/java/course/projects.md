The mini projects in each tab teach one topic at a time. This tab is the **one big project** that combines **all** of them: Core Java, Advanced Java, SQL, Spring Boot, React, Microservices, Kafka, DevOps, Testing, SE practices, DSA and System Design. You build it in phases alongside the 24-week roadmap, so by the end you have a single professional-grade project you can talk about for a whole interview.

## ⭐ ShopSphere: a full e-commerce platform

**What it is:** an online store like a small Amazon/Flipkart. Customers browse products, search, add to cart, check out and track orders. Admins manage products, stock, orders and see sales dashboards. There's also a flash-sale feature, where thousands of users try to buy limited stock at the same time.

**Why this project:** everyone understands e-commerce. It naturally needs every skill in this guide (data modelling, concurrency, security, events, scaling), and it matches the most common microservices and system design interview questions.

**Final architecture:** React storefront + React admin panel → API Gateway → 9 Spring Boot microservices (auth/user, catalog, search, cart, order, inventory, payment-mock, review, notification) → PostgreSQL, Redis, Kafka, OpenSearch → running in Docker/Kubernetes, deployed with CI/CD and monitored with Prometheus + Grafana + tracing.

## Coverage map: every tab → where it's used in ShopSphere

| Topic (tab) | Where ShopSphere uses it |
|---|---|
| **☕ Core Java** | Domain model with classes, records, enums and a sealed `OrderStatus`; `BigDecimal` for all prices and taxes; streams for cart totals, invoices and reports; custom exceptions (`OutOfStockException`, `InvalidCouponException`); Date/Time API for delivery estimates and sale windows; CSV parser for bulk product import (file I/O); Maven multi-module build. |
| **⚙️ Advanced Java** | Product page loads details, reviews, stock and recommendations in parallel with `CompletableFuture`; virtual threads enabled on I/O-heavy services; flash-sale stock reservation that is safe under heavy concurrency; thread-safe in-memory rate limiter; custom `@Audited` annotation implemented with AOP/reflection; profile a slow endpoint with JFR and fix it. |
| **🗄️ SQL & DB** | Normalised schema (users, addresses, products, categories, variants, carts, orders, order_items, payments, reviews) with an ER diagram; Flyway migrations; admin reports with window functions and CTEs (top 5 products per category, monthly revenue, repeat customers, running totals); indexes tuned with EXPLAIN ANALYZE; optimistic locking on inventory; a deliberate isolation level for checkout; Redis for cart and cache. |
| **🌱 Spring Boot** | REST APIs with validation and ProblemDetail errors; JPA relationships with N+1 fixed; search/filter/pagination with Specifications; Spring Security with JWT + roles (CUSTOMER, ADMIN); Keycloak OAuth2 later; `@Cacheable` with Redis; `@Scheduled` abandoned-cart emails; Spring Batch bulk product import; image upload; OpenAPI docs; Actuator. |
| **⚛️ React** | Storefront: catalogue with filters, debounced search with autocomplete, product page, cart, multi-step checkout form (React Hook Form + Zod), order history and live order tracking over WebSocket. Admin panel: product CRUD, order management, sales charts. TypeScript, React Router, protected routes, TanStack Query, Zustand for the cart, lazy-loaded routes, responsive Tailwind UI. |
| **🧩 Microservices** | Split the monolith into 9 services with database-per-service; Spring Cloud Gateway (routing, JWT check, rate limiting); service discovery; centralised config; Saga for checkout (reserve stock → pay → confirm, or compensate); transactional outbox; Resilience4j circuit breakers, timeouts and retries; correlation IDs. |
| **📨 Kafka** | Order lifecycle, inventory and payment events driving the Saga; notification-service consumes events to send emails; retry topics, dead-letter topic and idempotent consumers; Debezium CDC keeps the OpenSearch product index in sync; clickstream events → Kafka Streams → "Trending now" products; Avro + Schema Registry. |
| **🐳 DevOps & Cloud** | Multi-stage Dockerfiles; Docker Compose for the whole stack; Kubernetes manifests or Helm (kind/minikube); GitHub Actions CI/CD (test → scan → build image → push → deploy); deployment to AWS (ECS/EC2 + RDS + S3/CloudFront) or free hosting; Prometheus, Grafana, OpenTelemetry tracing, centralised logs. |
| **🧪 Testing** | JUnit 5 + Mockito unit tests (pricing, discounts, Saga steps); @WebMvcTest and @DataJpaTest; Testcontainers with Postgres, Kafka and Redis; REST Assured API tests; Vitest + React Testing Library; Playwright end-to-end checkout test; Gatling load test of the flash sale proving no overselling; JaCoCo coverage + SonarQube quality gate in CI. |
| **📐 SE Practices & Agile** | Run the build as Scrum on a GitHub Projects board (epics, user stories with acceptance criteria, story points, 2-week sprints, retro notes); trunk-based or Git Flow branching with PRs, a PR template and Conventional Commits; semantic-versioned releases with a changelog; ADRs for big decisions; a postmortem for an incident you inject on purpose. Design patterns used: Strategy (discounts, shipping), Factory (payment providers), Builder (orders), State (order lifecycle), Observer (events), Adapter (payment gateway), Decorator (pricing add-ons). SOLID throughout. |
| **🧠 DSA** | Trie for search autocomplete; LRU cache for "recently viewed"; heap/top-K for trending products; token-bucket rate limiter; graph + Dijkstra to pick the nearest warehouse; binary search for price-range filtering on sorted data. Implement them in Java with tests before using libraries. |
| **🏗️ System Design** | A written design doc: requirements, capacity estimates, high-level diagram, data model, caching strategy, consistency choices (CAP), scaling plan for the flash sale (queueing, Redis stock counters), failure scenarios. LLD class diagrams for cart, order and the discount engine. This is your answer to "design an e-commerce system". |

## Build plan: 7 phases matched to the roadmap

Each phase ends with a working, tested, committed version and a short LinkedIn post. Never break what already works; tag a release (v0.1, v0.2…) at the end of each phase.

### Phase 0 (weeks 1–6): *shopsphere-core*, pure JavaCore Java · Advanced Java · DSA

- Plain Java + Maven library, no Spring: Product, Category, Cart, Order, Customer, Coupon domain classes.
- Pricing engine: taxes, coupons, discount rules with the Strategy pattern, all in BigDecimal.
- CSV product importer with validation and an error report.
- Trie autocomplete, LRU "recently viewed", top-K bestsellers with a heap.
- Concurrency: simulate 1,000 threads buying 50 units of a flash-sale item, show the race condition, then fix it (synchronized → locks → atomics) and compare.
- JUnit 5 tests for everything. **Done when:** 80%+ coverage, a clean README, published on GitHub.

### Phase 1 (weeks 7–10): Spring Boot monolith backendSpring · SQL · Security · Testing

- Reuse the Phase 0 domain inside a Spring Boot app with a modular package structure (catalog, cart, order, user).
- PostgreSQL schema + Flyway; JPA entities and relationships; Specifications for search, filters and pagination.
- REST APIs, validation, ProblemDetail errors, MapStruct DTOs, OpenAPI docs.
- JWT auth with CUSTOMER/ADMIN roles; Redis cache for the catalogue; @Scheduled abandoned-cart job; Spring Batch product import.
- Admin SQL reports with window functions; tune indexes with EXPLAIN ANALYZE.
- **Done when:** unit + slice + Testcontainers tests pass and Swagger shows every endpoint.

### Phase 2 (weeks 11–13): React storefront + admin, deploy v1React · TypeScript · DevOps basics

- Storefront: home, catalogue with filters, search with autocomplete, product page, cart, checkout, order history.
- Admin: product CRUD with image upload, order list and status updates, sales charts.
- TanStack Query, Zustand cart, React Hook Form + Zod, protected routes, lazy loading, Vitest + RTL tests.
- Docker Compose for the whole stack; deploy (backend on Render/Railway/AWS, frontend on Vercel/Netlify).
- **Done when:** live demo link works end to end. **Start applying for jobs now.**

### Phase 3 (weeks 14–15): split into microservicesMicroservices

- Extract user, catalog, cart, order, inventory, payment-mock, review and notification services, each with its own database.
- Spring Cloud Gateway, service discovery, centralised config; Keycloak for OAuth2/OIDC login.
- Synchronous calls with RestClient/OpenFeign + Resilience4j (timeouts, retries, circuit breaker, fallbacks).
- Write ADRs explaining why and how you split it.
- **Done when:** the React app works unchanged through the gateway.

### Phase 4 (weeks 16–17): event-driven with KafkaKafka · Saga · Outbox

- Checkout Saga over Kafka: OrderCreated → StockReserved → PaymentCompleted → OrderConfirmed, with compensation on failure (the payment mock fails ~20% of the time).
- Transactional outbox (+ Debezium); idempotent consumers; retry topics and a dead-letter topic with a re-process admin endpoint.
- notification-service emails customers; WebSocket pushes live order status to React.
- search-service: Debezium CDC → Kafka → OpenSearch for full-text product search.
- Clickstream → Kafka Streams → "Trending now" widget on the home page.
- **Done when:** killing any one service mid-checkout never leaves stock or orders in a wrong state.

### Phase 5 (weeks 18–20): production setupDocker · Kubernetes · CI/CD · Cloud · Observability

- Kubernetes manifests/Helm with probes, ConfigMaps, Secrets, autoscaling; run on kind/minikube.
- GitHub Actions: PR checks (tests, JaCoCo, Sonar), images pushed to a registry on merge, automatic deploy.
- Prometheus metrics, Grafana dashboards (orders/min, error rate, consumer lag), OpenTelemetry traces across services.
- Cloud deployment on AWS (stay on the free tier) or a cheaper alternative, documented step by step.
- **Done when:** a push to main reaches a running environment automatically and you can trace one checkout across all services.

### Phase 6 (weeks 21–24): harden, measure and presentTesting · System Design · SE Practices

- Flash-sale load test with Gatling (e.g. 5,000 users, 100 units): prove there's no overselling; try Redis atomic counters vs DB locking and record the numbers.
- Playwright end-to-end checkout test in CI.
- Security pass against the OWASP Top 10; dependency scanning.
- Inject an incident (e.g. Kafka consumer stuck), handle it, write a blameless postmortem.
- Final design doc + architecture diagrams + a 3–5 minute demo video + a polished README.
- **Done when:** you can whiteboard the whole system and explain every trade-off in 10 minutes.

## Repository structure

shopsphere/ ├── docs/ → design doc, ADRs, diagrams, postmortems ├── core/ → Phase 0 pure-Java domain & algorithms ├── services/ │ ├── gateway/ user/ catalog/ search/ cart/ │ ├── order/ inventory/ payment-mock/ review/ notification/ ├── frontend/ │ ├── storefront/ (React + TS) │ └── admin/ (React + TS) ├── infra/ │ ├── docker-compose.yml │ ├── k8s/ or helm/ │ └── monitoring/ (Prometheus, Grafana dashboards) ├── tests/ → Gatling load tests, Playwright e2e └── .github/workflows/ → CI/CD pipelines

## How to show it on your resume and in interviews

- **Resume entry:** "ShopSphere: e-commerce platform with 9 Spring Boot microservices, Kafka-based checkout Saga with outbox, React + TypeScript storefront and admin, PostgreSQL/Redis/OpenSearch, Docker + Kubernetes, GitHub Actions CI/CD, Prometheus/Grafana monitoring."
- **Impact bullets (use your measured numbers):** "Prevented overselling under 5,000 concurrent buyers during load tests"; "Cut product page latency from X ms to Y ms with parallel calls + Redis caching"; "80%+ test coverage with Testcontainers integration tests".
- **Interview walkthrough (5 minutes):** the problem → architecture diagram → one hard problem (the flash-sale race condition or the Saga failure handling) → what you'd improve next.
- Keep the phase tags (v0.1–v1.0), so interviewers can see how the project grew from a monolith into microservices.

## Prefer a different theme? Same coverage, different domain

### 🍔 FoodDash: food delivery platform

- Restaurants, menus, cart, orders, delivery partners, live tracking.
- Kafka order events between restaurant, kitchen, delivery and notification services; Saga for order + payment.
- Dijkstra/nearest-partner assignment (DSA); WebSocket live map (React); surge handling at lunchtime (system design, concurrency).
- Same SQL, Spring, testing, DevOps and Scrum checklist as ShopSphere.

### 🎟️ EventHub: event and ticket booking platform

- Events, venues, seat maps, booking, waitlists, e-tickets with QR codes.
- Seat holds with TTL and locking so no seat is double-booked (concurrency); Kafka for booking, payment and email events.
- Interval trees/sorting for schedules (DSA); live seat map (React + WebSocket); traffic spikes when tickets go on sale (system design).
- Same SQL, Spring, testing, DevOps and Scrum checklist as ShopSphere.

**Rules:** (1) Commit small and often with clear messages; recruiters look at commit history. (2) Use tutorials for ideas, but write the code yourself and be able to explain every line. (3) Measure things (latency, throughput, coverage) and put the real numbers in the README. (4) Pin this repo first on your GitHub profile.
