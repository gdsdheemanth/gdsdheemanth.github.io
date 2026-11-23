The heart of the backend. Learn how Spring works underneath (IoC, proxies, auto-configuration), not only the annotations.

This track lists every topic to learn, in order, with mini projects to build and the best resources. Work through the topics with the resources below, then build at least two of the projects.

## Spring Core

### Inversion of Control & Dependency Injection (constructor vs setter vs field injection)

Learn this from the resources at the end of the section.

### ApplicationContext, BeanFactory, bean definition

Learn this from the resources at the end of the section.

### Stereotypes: @Component, @Service, @Repository, @Controller; @Configuration + @Bean

Learn this from the resources at the end of the section.

### @Autowired, @Qualifier, @Primary; resolving multiple beans

Learn this from the resources at the end of the section.

### Bean scopes (singleton, prototype, request, session) and bean lifecycle (@PostConstruct, @PreDestroy)

Learn this from the resources at the end of the section.

### Profiles, @Value, @ConfigurationProperties, externalized configuration

Learn this from the resources at the end of the section.

### Spring AOP: aspects, advice types, pointcuts, how proxies work

Learn this from the resources at the end of the section.

### Circular dependencies & how to avoid them

Learn this from the resources at the end of the section.


## Spring Boot

### Spring Initializr, starters, project structure

Learn this from the resources at the end of the section.

### How auto-configuration works (@Conditional…, @SpringBootApplication breakdown)

Learn this from the resources at the end of the section.

### application.yml/properties, profiles, environment-specific config

Learn this from the resources at the end of the section.

### Embedded server, executable JAR, DevTools

Learn this from the resources at the end of the section.

### Actuator: health, metrics, info endpoints

Learn this from the resources at the end of the section.

### Logging configuration

Learn this from the resources at the end of the section.

### Boot 2 → 3 → 4 changes: javax → jakarta, Java 17 baseline, observability, native images (awareness)

Learn this from the resources at the end of the section.


## REST API development

### @RestController, @RequestMapping/@GetMapping…, @PathVariable, @RequestParam, @RequestBody

Learn this from the resources at the end of the section.

### ResponseEntity & proper HTTP status codes

Learn this from the resources at the end of the section.

### DTOs vs entities; mapping with MapStruct

Learn this from the resources at the end of the section.

### Validation with Jakarta Bean Validation (@Valid, @NotNull, custom validators)

Learn this from the resources at the end of the section.

### Global exception handling: @RestControllerAdvice, ProblemDetail (RFC 9457)

Learn this from the resources at the end of the section.

### Pagination, sorting, filtering; API versioning strategies

Learn this from the resources at the end of the section.

### OpenAPI / Swagger UI with springdoc-openapi

Learn this from the resources at the end of the section.

### File upload/download (MultipartFile), CORS configuration

Learn this from the resources at the end of the section.

### Calling other APIs: RestClient, WebClient, OpenFeign

Learn this from the resources at the end of the section.

### Idempotency and REST best practices

Learn this from the resources at the end of the section.


## Data access: Spring Data JPA & Hibernate

### JPA vs Hibernate vs Spring Data JPA

Learn this from the resources at the end of the section.

### Entities, @Id strategies, @Column, embeddables

Learn this from the resources at the end of the section.

### Relationships: @OneToOne, @OneToMany, @ManyToOne, @ManyToMany; owning side; cascade; orphanRemoval

Learn this from the resources at the end of the section.

### Fetch types (LAZY vs EAGER), LazyInitializationException

Learn this from the resources at the end of the section.

### N+1 problem & fixes: JOIN FETCH, @EntityGraph, batch size

Top interview question.

### Repositories: derived queries, @Query (JPQL & native), projections, Specifications

Learn this from the resources at the end of the section.

### Entity lifecycle states, persistence context, first-level & second-level cache

Learn this from the resources at the end of the section.

### @Transactional: propagation, isolation, rollback rules, self-invocation pitfall

Learn this from the resources at the end of the section.

### Optimistic locking (@Version), pessimistic locking

Learn this from the resources at the end of the section.

### Auditing (@CreatedDate, @LastModifiedBy); DB migrations with Flyway or Liquibase

Learn this from the resources at the end of the section.


## Spring Security

### Authentication vs authorization; the SecurityFilterChain

Learn this from the resources at the end of the section.

### UserDetailsService, PasswordEncoder (BCrypt)

Learn this from the resources at the end of the section.

### Stateless JWT authentication end to end

Learn this from the resources at the end of the section.

### OAuth2 / OpenID Connect: resource server, login with Keycloak/Google

Learn this from the resources at the end of the section.

### Role- and method-level security (@PreAuthorize)

Learn this from the resources at the end of the section.

### CSRF, CORS, session vs token security; OWASP Top 10 basics

Learn this from the resources at the end of the section.


## More Spring features

### Caching with @Cacheable + Redis

Learn this from the resources at the end of the section.

### Scheduling (@Scheduled) and async processing (@Async)

Learn this from the resources at the end of the section.

### Spring Batch: jobs, steps, readers/processors/writers, chunking

For large file imports and batch jobs.

### Application events (@EventListener)

Learn this from the resources at the end of the section.

### Spring WebFlux / reactive programming: awareness level

Learn this from the resources at the end of the section.

## Hands-on mini projects

Build at least two of these before moving on. Put each on GitHub with a short README.

### URL Shortener API (Beginner)

Shorten URLs, redirect, track click counts, set expiry. Validation, global error handling, Swagger docs, Redis cache for hot links.

**Practises:** Spring Boot, REST, validation, Redis caching, OpenAPI

### Blog / CMS REST API (Intermediate)

Users, posts, comments, tags; pagination, sorting, search; JPA relationships; Flyway migrations; JWT login with USER/ADMIN roles.

**Practises:** Spring Data JPA, N+1 fixes, Spring Security, JWT

### Job Board API (Intermediate)

Companies post jobs, candidates upload resumes (file upload) and apply, filters by skill/location, @Scheduled email digest of new jobs.

**Practises:** Multipart upload, Specifications, scheduling, email, @Async

### Expense Splitter (Splitwise-style) API (Intermediate)

Groups, shared expenses, equal/exact/percentage splits, balances and settle-up suggestions. Strong unit tests on the splitting logic.

**Practises:** Domain modelling, BigDecimal, @Transactional, testing

### OAuth2 Login + Keycloak demo (Advanced)

Secure an API as an OAuth2 resource server with Keycloak; add Google login in a client app; method-level security.

**Practises:** OAuth2/OIDC, Spring Security internals

## Resources

::: resources
- [Spring Guides](https://spring.io/guides) · Official 15-minute hands-on guides
- [Spring Boot Reference](https://docs.spring.io/spring-boot/index.html) · Official documentation
- [Spring Academy](https://spring.academy/courses) · Free official courses (Spring Boot, Spring Security…)
- [Baeldung: Spring](https://www.baeldung.com/spring-tutorial) · Huge library of Spring articles
- Video: [Dan Vega (Spring Developer Advocate)](https://www.youtube.com/results?search_query=Dan+Vega+Spring+Boot) · Up-to-date Spring Boot 3/4 videos
- Video: [Amigoscode: Spring Boot](https://www.youtube.com/results?search_query=Amigoscode+Spring+Boot+full+course) · Project-based full courses
- Video: [Java Brains: Spring](https://www.youtube.com/results?search_query=Java+Brains+Spring+Boot) · Clear concept explanations
- Book: [Spring Start Here, Laurențiu Spilcă](https://www.manning.com/books/spring-start-here) · Best beginner book on Spring fundamentals
- Book: [Spring Security in Action (2nd ed.)](https://www.manning.com/books/spring-security-in-action-second-edition) · Security deep dive
- Book: [Spring in Action, Craig Walls](https://www.manning.com/books/spring-in-action-sixth-edition) · Broad Spring coverage
:::
