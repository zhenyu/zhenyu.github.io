---
layout: post
title: "When We Talk About Workflow, What Are We Actually Talking About?"
date: 2026-05-18 10:00:00 -0700
categories: [ml-infrastructure, platform-engineering]
tags: [Workflow, Airflow, Argo, Kubeflow, Flyte, Agent, MLOps]
excerpt: "Airflow, Argo, Kubeflow Pipelines, Flyte, Dify, and Google ADK all call themselves workflow systems. They are not interchangeable. The useful question is not which one is the best workflow engine, but what execution contract each family represents."
---

I've been having the same conversation a lot lately. Someone says "we're picking a workflow engine," and the candidates on the table are Airflow, Argo, Kubeflow Pipelines, Flyte, Dify, and Google's ADK. They are often compared as if they are alternatives to one another. They are not, at least not in the way the shared label suggests.

This post is my attempt to make that observation useful rather than pedantic. I want to sort these systems into a few workflow families, using representative examples rather than attempting an exhaustive product comparison. Airflow, Argo, KFP, Flyte, Dify, and ADK are useful because each makes a different execution contract visible. The point is not that these are the only workflow systems worth discussing, or that every deployment of each system behaves exactly the same way. The point is that the word *workflow* hides several very different architectural choices.

The two axes I find most useful are:

1. **What is the scheduling unit?**
2. **When does the DAG become real?**

A third question follows from those two: **what kind of durability contract does the system provide?**

These questions explain more than feature checklists. A workflow step may mean a function call inside one Python process, a task picked up by a long-running worker, or a new Pod created on a Kubernetes cluster. A DAG may be a Python file parsed by a scheduler, a compiled intermediate representation, a registered entity in a control plane, a Kubernetes custom resource, or an in-process graph traversed during a request. These are not small implementation differences. They define latency, isolation, retry semantics, operational complexity, and the kinds of workloads the system naturally fits.

Here is the simplified map before going into details:

| Workflow family | Representative examples | Typical scheduling unit | When the DAG becomes real | Optimized for |
|---|---|---|---|---|
| ETL workflow | Airflow | Airflow task executed through a configured executor / worker backend | DAG file parsed by the scheduler | scheduled batch reliability, dependency management, backfill |
| Kubernetes-native workflow | Argo Workflows | Kubernetes Pod / Kubernetes resource controlled by a Workflow CRD | Workflow CR submitted to Kubernetes | isolation, retry, artifact lineage, Kubernetes-native execution |
| ML pipeline on Kubernetes | Kubeflow Pipelines, Flyte | containerized task on a backend execution system | compiled IR or registered workflow entity | ML pipeline authoring, reproducibility, containerized execution |
| Agent workflow | Google ADK, Dify | in-process function / coroutine / sub-agent / graph node | request-time or graph-defined control flow | latency, interactivity, streaming model output |

This table is intentionally rough. Real systems have plugins, executors, backends, and deployment modes that can shift details. But the table captures the design center of each family. When teams skip this layer and compare all of them under the single label "workflow," they often end up evaluating the wrong thing.

## The two axes

### Axis 1: scheduling unit

When the engine "runs a step," what does that mean physically?

Is it a function call inside the same Python process? A task picked up by a long-running worker? A new Pod created on a Kubernetes cluster? A container launched by a pipeline backend? These choices have very different latency floors, isolation boundaries, and failure modes.

Most workflow systems have a design center on this axis. Some have multiple executors or deployment modes, but the design center still matters because it shapes the operational model users inherit.

### Axis 2: DAG plasticity

When is the DAG "real"?

Is it written in a file at deploy time and parsed by a scheduler? Is it compiled at registration time into an intermediate representation? Is it submitted as a Kubernetes custom resource? Is it traversed inside a single request, possibly while an LLM decides the next action?

"Static at deploy time" and "dynamic per request" are not merely points on a spectrum. They imply different control-plane assumptions. A scheduler that can backfill yesterday's failed ETL run is solving a different problem from an agent graph that streams tokens back to a user in one request.

### Consequence: durability contract

The scheduling unit and DAG materialization point usually determine the durability contract.

Airflow cares about scheduled task instances, retry, backfill, and operator visibility. Argo cares about Kubernetes resources, Pod lifecycle, artifact passing, and workflow status. KFP and Flyte care about reproducible pipeline runs and containerized tasks. Agent workflows care about low-latency orchestration inside an interactive request.

A system can sometimes be extended beyond its design center. But if the durability contract does not match the workload, the integration tends to feel unnatural.

## The ETL model: Airflow and the self-managed worker tradition

Airflow is the oldest system in this discussion, and for a long time it was *the* workflow engine in many data teams: the default answer when people meant scheduled ETL, dependency management, retries, and backfills.

Airflow's architecture is organized around a scheduler, task instances, metadata state, and a configured executor. The scheduler decides when a task is ready; the executor determines how the task is physically run. In a historically common production setup, `CeleryExecutor` dispatches tasks to long-running Celery workers. In other deployments, `LocalExecutor`, `KubernetesExecutor`, or hybrid executors can change the physical execution backend.

So the careful statement is not that Airflow can only run tasks on Airflow-owned workers. It cannot; `KubernetesExecutor` exists and launches Pods. The more useful statement is that Airflow's design center is an **Airflow task instance managed by the Airflow scheduler/executor model**, rather than a Kubernetes-native Workflow custom resource.

DAG plasticity is also on the static side. A DAG is usually a Python file in a DAG folder; the scheduler parses it and creates task instances according to schedules and dependencies. Airflow 2.x introduced dynamic task mapping, which is useful for runtime fan-out over collections, but it does not turn Airflow into a per-request dynamic agent runtime.

This is not a criticism of Airflow. Airflow is well suited to what it was originally built for: scheduled batch workflows where individual tasks may take minutes or hours, reliability matters more than per-hop latency, and backfill is a first-class operational requirement.

The mismatch appears when Airflow is pressed into workloads with different contracts: tight ML training loops, interactive serving paths, or LLM agent orchestration where a few seconds of orchestration latency is already too much.

## The Kubernetes-native model: from worker pool to Pod

Once Kubernetes became the substrate for many ML and data platforms, a different question became natural:

> If Kubernetes already schedules Pods with quotas, node selectors, topology, and resource constraints, why should a workflow system maintain its own worker pool inside the cluster?

Argo Workflows is a representative answer to that question.

In Argo, the design center is Kubernetes-native execution. A workflow is a Kubernetes custom resource. The controller reconciles that resource. Each step is typically represented by a Pod or Kubernetes execution primitive, and `kube-scheduler` decides where it lands.

This shifts the workflow engine out of the placement business. Argo defines workflow structure, dependencies, retries, parameters, and artifacts; Kubernetes handles Pod placement. That distinction matters. If a platform's real problem is GPU quota, gang scheduling, topology-aware placement, or cluster-level admission control, changing workflow engines alone is unlikely to solve it. Those concerns sit closer to the Kubernetes scheduler, resource admission, Kueue, Volcano, or platform-specific placement logic.

Argo's DAG also lives outside the user container. A workflow is submitted as a custom resource. Because steps are separate Pods, artifact passing becomes the natural communication pattern. A producing step writes declared output artifacts; the workflow system stores them in a configured artifact repository; a downstream step reads them as inputs.

This is strong for reproducibility, retry, and isolation. It is not the same as an in-memory streaming dataflow engine like Spark or Flink. Both may draw DAGs, but the data movement contract is different.

That distinction explains a common platform confusion: a workflow DAG is not automatically a data-processing DAG. Argo can orchestrate steps that process data. It does not, by itself, become a high-throughput distributed data engine with shuffle, pipelined execution, and memory-aware data movement.

## Pythonic ML pipelines on top of workflow backends

Raw YAML is not the interface most ML engineers want. That created a second layer of systems: Pythonic ML pipeline authoring systems that compile or register workflows onto a backend execution system.

Kubeflow Pipelines and Flyte are representative examples, but they choose noticeably different shapes.

### Kubeflow Pipelines: Python as an authoring and compilation layer

In the classic standalone KFP v1 deployment, Kubeflow Pipelines used Argo Workflows as the workflow engine. That does not mean every modern KFP-conformant backend is Argo. KFP v2's IR and conformant-backend model explicitly decouple authoring from a single execution backend.

The more durable architectural point is this: KFP treats Python primarily as an **authoring and compilation layer**.

You write `@dsl.component` and `@dsl.pipeline` functions in Python. The SDK compiles the pipeline into an intermediate representation. At run creation time, the backend turns that representation into whatever execution format it supports. In the Argo-backed deployment, that means Argo Workflow resources. In other backends, it may mean something else.

The important property is that the Python pipeline definition is not the runtime process executing every step. Task code is packaged into containers. The pipeline topology has already been compiled into a backend-consumable form. Main and task are not running in the same Python execution context.

This makes KFP feel different from both Airflow and in-process agent frameworks. It is not a scheduler parsing DAG files forever, and it is not a request-time control loop. It is an ML pipeline authoring layer with a compiled execution representation.

### Flyte: Python as a registration-time entity

Flyte is another route to Pythonic ML/data workflows, but it has a different shape.

Flyte makes tasks and workflows Python-decorated entities that are registered with the control plane. At registration time, Flyte serializes task definitions, workflow structure, image references, interface definitions, and other execution metadata into a registered entity.

This gives Flyte a strong type-aware, reproducible, control-plane-centric model. It also means SDK ergonomics matter a lot. If image dependencies, runtime configuration, copied source files, Spark configuration, and workflow structure are colocated in decorators and registration metadata, ordinary operational changes can enter the image or registration loop.

That is not an inherent limitation of Flyte. Flyte provides mechanisms such as reusable container images, `ContainerTask`, fast registration, and other ways to separate orchestration from image lifecycle. A disciplined team can keep those layers clean.

The point is narrower: Flyte's design center makes Python a registration-time source of truth, not just a YAML generator and not an in-process request runtime. That gives it a different coupling surface from KFP. KFP tends to make Python disappear into compiled IR; Flyte tends to preserve Python-defined entities as registered control-plane objects.

That difference matters when thinking about iteration loops, image lifecycle, and the boundary between business logic, orchestration metadata, and runtime environment.

## Workflow is not ML pipeline, and ML pipeline is not MLOps

This is the section I think many ML infrastructure discussions skip too quickly.

A pipeline engine solves one slice of the problem: how steps connect to each other and run in order. That is useful, but it is not the whole of MLOps.

MLOps, if the term means anything operationally, includes more than step orchestration:

- experiment tracking
- run comparison
- model registry
- lineage from data and code to model artifact
- distributed training job integration
- serving and deployment integration
- notebook and development environments
- data and artifact versioning
- reproducible runtime environments

KFP runs pipelines. Flyte runs workflows. Argo runs workflows. Airflow runs scheduled DAGs. None of these, on its own, is a complete MLOps platform.

Kubeflow as a broader distribution is closer to a platform because it includes components beyond KFP: training operators, Katib, KServe, notebooks, model registry work, and integrations around the ML lifecycle. But KFP-the-pipeline-engine and Kubeflow-the-ML-platform are not the same thing.

This distinction matters because teams often install a pipeline engine and assume they have installed MLOps. They have not. They have installed orchestration. They still need to decide how experiments are tracked, how models are registered, how training jobs report metrics, how lineage is preserved, how artifacts are promoted, and how serving is connected.

Installing a pipeline engine and calling it MLOps is a bit like installing `make` and calling it CI/CD.

## The agent model: the scheduling unit shrinks back into the process

The newest source of confusion is the agent ecosystem adopting the word "workflow."

This is not wrong exactly. Agent frameworks do orchestrate steps. But their execution contract is very different from Airflow, Argo, KFP, or Flyte.

Google ADK's older Workflow Agents, such as `SequentialAgent`, `ParallelAgent`, and `LoopAgent`, were deterministic templates for sub-agent execution. Current ADK documentation says these template workflows have been superseded by graph-based and dynamic workflows starting in ADK 2.0. That changes the API surface and flexibility story, but it does not make ADK equivalent to Airflow or Argo.

The design center is still low-latency orchestration of agents, tools, and graph nodes inside an application/request context. A `ParallelAgent`-style fan-out is closer to coroutine concurrency over sub-agents than to Kubernetes scheduling. It is useful for parallel LLM/tool calls. It is not the same thing as launching independent Pods across a cluster with durable artifact lineage.

Dify Workflows sit in a similar broad family from an architecture perspective. Dify may persist workflow runs and use background workers for parts of the application, but it should not be confused with a durable batch scheduler like Airflow or a Kubernetes-native workflow controller like Argo. Its design center is application-level orchestration for LLM apps: nodes, variables, prompts, tools, model calls, and user-facing execution.

This explains the practical mismatch.

Putting an LLM agent loop on Airflow introduces a scheduler and task-instance model into a path that wants low-latency token streaming. Putting a daily ETL job on an agent workflow framework gives up the operational contract that ETL teams usually need: backfill, long-running scheduler state, retry policy, historical observability, and failure handling at 3 AM.

Both systems may call the thing a workflow. They are not solving the same problem.

## A more useful comparison

The systems become easier to compare if we stop asking which one is the best workflow engine and instead ask what execution contract each one gives us.

| Question | Airflow-style ETL | Argo-style K8s workflow | KFP / Flyte ML pipeline | Agent workflow |
|---|---|---|---|---|
| What is the scheduling unit? | Airflow task through executor / worker backend | Kubernetes Pod or resource | containerized task on backend | in-process function, coroutine, sub-agent, graph node |
| Where is the control plane? | Airflow scheduler and metadata DB | Kubernetes API + workflow controller | pipeline control plane / backend | application process / agent runtime |
| When is DAG real? | parsed from DAG files by scheduler | submitted as Workflow CR | compiled IR or registered entity | request-time or graph-defined execution |
| Latency floor | seconds-scale is normal | Pod startup scale | backend/container startup scale | sub-second to request-scale |
| Durability model | task instance state, retry, backfill | workflow status, Pod state, artifacts | pipeline run metadata, task artifacts | application/run-level persistence, usually not batch scheduler semantics |
| Best fit | scheduled ETL and batch operations | Kubernetes-native batch orchestration | reproducible ML pipelines | interactive LLM applications and agent orchestration |

This is still simplified, but it is much more useful than a flat feature comparison.

A few examples follow directly:

- If you need backfill and scheduled dependency management, start with Airflow-style assumptions.
- If you need Pod-level isolation and Kubernetes-native execution, start with Argo-style assumptions.
- If you need reproducible containerized ML pipelines with Python authoring, start with KFP/Flyte-style assumptions.
- If you need low-latency LLM application control flow, start with agent-workflow assumptions.

A system can sometimes be stretched outside its design center, but the stretch should be explicit. Otherwise the team mistakes a naming overlap for architectural compatibility.

## A small test before choosing anything called a workflow

Before committing to any workflow system, I would ask these questions in order.

### 1. What is the scheduling unit?

A worker process? A Kubernetes Pod? A containerized backend task? An in-process function call? A coroutine? A sub-agent?

This tells you the latency floor, isolation model, resource boundary, and most of the operational shape.

### 2. When does the DAG become real?

A Python file parsed by a scheduler? A YAML custom resource submitted to Kubernetes? A compiled IR? A registered entity? A graph traversed inside a request?

This tells you how dynamic the system really is, and where the control plane lives.

### 3. What durability contract do you need?

Do you need retry and backfill? Artifact lineage? Kubernetes-native status? ML pipeline run metadata? Or do you mostly need request-local orchestration and streaming?

Durability is not a feature checkbox. It is part of the workload contract.

### 4. Are you actually asking for workflow, ML pipeline, or MLOps?

A workflow engine runs ordered steps. An ML pipeline gives ML-oriented authoring and reproducibility around those steps. MLOps includes the broader lifecycle: experiments, model registry, deployment, lineage, serving, and governance.

If the real problem is MLOps, choosing a workflow engine is only one part of the answer.

## Closing thought

The label "workflow" is fine as marketing, but weak as an architecture tool.

It does not tell you what the scheduling unit is. It does not tell you when the DAG becomes real. It does not tell you what durability contract the system provides. It does not tell you whether the system is built for scheduled ETL, Kubernetes-native batch execution, reproducible ML pipelines, or interactive agent applications.

So I would not start with:

> Which workflow engine should we use?

I would start with:

> What execution contract does this workload need?

Once that is clear, the comparison becomes much less confusing. Airflow, Argo, KFP, Flyte, Dify, and ADK are not just competing products under one label. They are representative points in different workflow families. Treating them that way makes the design discussion more honest.
