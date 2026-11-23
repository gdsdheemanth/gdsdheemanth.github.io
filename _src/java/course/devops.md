A full stack developer is expected to ship code: version control, containers, pipelines and basic cloud.

This track lists every topic to learn, in order, with mini projects to build and the best resources. Work through the topics with the resources below, then build at least two of the projects.

## Linux & tools

### Shell basics: navigation, files, permissions, processes, pipes, grep, environment variables

Learn this from the resources at the end of the section.

### SSH, curl, reading logs on servers

Learn this from the resources at the end of the section.


## Git & collaboration

### init, clone, add, commit, push, pull, branch, checkout/switch

Learn this from the resources at the end of the section.

### Merge vs rebase; resolving conflicts

Learn this from the resources at the end of the section.

### reset vs revert, stash, cherry-pick, reflog

Learn this from the resources at the end of the section.

### Pull requests & code review flow on GitHub/GitLab/Bitbucket

Learn this from the resources at the end of the section.

### Branching strategies: Git Flow vs trunk-based; Conventional Commits

Learn this from the resources at the end of the section.


## Containers

### Docker concepts: image, container, layers, registry

Learn this from the resources at the end of the section.

### Writing a Dockerfile for Spring Boot (multi-stage builds, layered JARs)

Learn this from the resources at the end of the section.

### Docker Compose: app + Postgres + Kafka + Redis locally

Learn this from the resources at the end of the section.

### Volumes, networks, environment variables

Learn this from the resources at the end of the section.


## Kubernetes basics

### Pods, Deployments, ReplicaSets, Services, Ingress

Learn this from the resources at the end of the section.

### ConfigMaps & Secrets, resource limits, probes

Learn this from the resources at the end of the section.

### Scaling & rolling updates; Helm: awareness

Learn this from the resources at the end of the section.


## CI/CD

### CI/CD concepts: build → test → scan → deploy

Learn this from the resources at the end of the section.

### GitHub Actions workflow for a Maven + React project

Learn this from the resources at the end of the section.

### Jenkins: awareness (very common in large companies)

Learn this from the resources at the end of the section.

### Code quality gates: SonarQube

Learn this from the resources at the end of the section.


## Cloud (AWS focus)

### Core services: EC2, S3, RDS, IAM, VPC basics

Learn this from the resources at the end of the section.

### Containers on AWS: ECS / EKS; serverless Lambda awareness

Learn this from the resources at the end of the section.

### CloudWatch logs & metrics

Learn this from the resources at the end of the section.

### Networking basics: load balancers, reverse proxy (Nginx), TLS/HTTPS

Learn this from the resources at the end of the section.

## Hands-on mini projects

Build at least two of these before moving on. Put each on GitHub with a short README.

### Dockerize a full stack app (Beginner)

Multi-stage Dockerfiles for your Spring Boot API and React app + Docker Compose with Postgres and Redis. One command starts everything.

**Practises:** Docker, Compose, networks, volumes

### CI/CD Pipeline with GitHub Actions (Intermediate)

On every PR: build, test, Sonar scan. On merge to main: build images, push to GitHub Container Registry, auto-deploy.

**Practises:** GitHub Actions, pipelines, registries

### Kubernetes Deployment (Intermediate)

Deploy the app to kind/minikube: Deployments, Services, Ingress, ConfigMaps, Secrets, probes, then scale replicas and do a rolling update.

**Practises:** Kubernetes basics

### Deploy to AWS (Advanced)

Run the backend on EC2 or ECS with RDS PostgreSQL, the frontend on S3 + CloudFront, logs in CloudWatch. Write down the setup steps and costs (stay on the free tier).

**Practises:** AWS EC2/ECS, RDS, S3, IAM

## Resources

::: resources
- [Pro Git book](https://git-scm.com/book/en/v2) · Complete Git book, free
- [Learn Git Branching](https://learngitbranching.js.org/) · Visual, interactive Git practice
- [Docker: Get Started](https://docs.docker.com/get-started/) · Official Docker tutorial
- [Kubernetes Tutorials](https://kubernetes.io/docs/tutorials/) · Official K8s basics
- [GitHub Actions docs](https://docs.github.com/en/actions) · CI/CD pipelines
- [AWS Skill Builder](https://skillbuilder.aws/) · Free AWS training, incl. Cloud Practitioner
- [The Missing Semester (MIT)](https://missing.csail.mit.edu/) · Shell, Git, tools every dev needs
- [Linux Journey](https://linuxjourney.com/) · Learn Linux basics
- Video: [TechWorld with Nana](https://www.youtube.com/results?search_query=TechWorld+with+Nana+Docker+Kubernetes) · Docker, K8s, CI/CD explained clearly
- [roadmap.sh: DevOps](https://roadmap.sh/devops) · Know what's out there
:::
