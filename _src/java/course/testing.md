Tests in your projects send a strong signal. Many junior candidates skip them, so this is an easy way to stand out.

This track lists every topic to learn, in order, with mini projects to build and the best resources. Work through the topics with the resources below, then build at least two of the projects.

## Foundations

### Testing pyramid: unit, integration, end-to-end

Learn this from the resources at the end of the section.

### TDD basics: red → green → refactor

Learn this from the resources at the end of the section.

### What makes a good test (fast, isolated, deterministic, readable)

Learn this from the resources at the end of the section.


## Java testing

### JUnit 5: annotations, assertions, lifecycle, @ParameterizedTest, @Nested

Learn this from the resources at the end of the section.

### AssertJ fluent assertions

Learn this from the resources at the end of the section.

### Mockito: mock vs spy, stubbing, verify, ArgumentCaptor, @InjectMocks

Learn this from the resources at the end of the section.

### When not to mock (don't mock what you don't own)

Learn this from the resources at the end of the section.


## Spring Boot testing

### @SpringBootTest vs test slices (@WebMvcTest, @DataJpaTest)

Learn this from the resources at the end of the section.

### MockMvc for controller tests; @MockitoBean

Learn this from the resources at the end of the section.

### Testcontainers for real PostgreSQL & Kafka in tests

Learn this from the resources at the end of the section.

### API testing with Postman and REST Assured

Learn this from the resources at the end of the section.


## Frontend & beyond

### Vitest + React Testing Library

Learn this from the resources at the end of the section.

### E2E testing: Playwright or Cypress (awareness)

Learn this from the resources at the end of the section.

### Performance/load testing: JMeter, Gatling (awareness)

Learn this from the resources at the end of the section.


## Code quality

### Code coverage with JaCoCo (and why 100% isn't the goal)

Learn this from the resources at the end of the section.

### Static analysis: SonarQube/SonarLint, Checkstyle, SpotBugs

Learn this from the resources at the end of the section.

### Formatting & linting (Spotless, ESLint, Prettier)

Learn this from the resources at the end of the section.

## Hands-on mini projects

Build at least two of these before moving on. Put each on GitHub with a short README.

### TDD Katas (Beginner)

Do the String Calculator, Bowling Game and Roman Numerals katas strictly test-first (red → green → refactor).

**Practises:** JUnit 5, TDD habit

### Test-cover your Spring project (Intermediate)

Take your Blog/Job Board API to 80%+ service coverage: unit tests with Mockito, @WebMvcTest for controllers, @DataJpaTest + Testcontainers for repositories, JaCoCo report in CI.

**Practises:** Mockito, test slices, Testcontainers, JaCoCo

### API test suite (Intermediate)

Write a Postman collection and a REST Assured suite covering happy paths, validation errors and auth failures; run it in CI.

**Practises:** API testing

### End-to-end tests (Advanced)

Playwright tests for login → create item → edit → delete in your React app, running against Docker Compose in CI.

**Practises:** E2E testing, CI

## Resources

::: resources
- [JUnit 5 User Guide](https://junit.org/junit5/docs/current/user-guide/) · Official JUnit docs
- [Mockito](https://site.mockito.org/) · Mocking framework docs
- [Testcontainers guides](https://testcontainers.com/guides/) · Real DB/Kafka in tests
- [Spring Boot testing docs](https://docs.spring.io/spring-boot/reference/testing/index.html) · Test slices, MockMvc
- [Practical Test Pyramid, Martin Fowler](https://martinfowler.com/articles/practical-test-pyramid.html) · How to structure tests
- [Testing Library (React)](https://testing-library.com/docs/react-testing-library/intro/) · Frontend component tests
- [Vitest](https://vitest.dev/guide/) · Test runner for Vite/React
- [Baeldung: Testing](https://www.baeldung.com/java-tests) · JUnit, Mockito, AssertJ articles
:::
