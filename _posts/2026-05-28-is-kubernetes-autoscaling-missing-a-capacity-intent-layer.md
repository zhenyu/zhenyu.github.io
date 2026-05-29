---
layout: post
title: "Is Kubernetes Autoscaling Missing a Capacity Intent Layer?"
date: 2026-05-28
categories: [ml-infrastructure, kubernetes, autoscaling]
tags: [kubernetes, autoscaling, karpenter, kueue, dra, cluster-autoscaler, ai-infrastructure, llm-inference]
---

A friend recently made a simple observation about Kubernetes autoscaling:

> Kubernetes can scale Pods, but turning those Pods into the right Nodes still feels like something every DevOps or platform team has to solve by hand.

That comment stuck with me.

At first, it sounds like an implementation complaint. Maybe HPA is too limited. Maybe Cluster Autoscaler is too reactive. Maybe Karpenter needs more policy. Maybe every platform team just needs better conventions around NodePools, labels, taints, instance types, and reservations.

But after looking at the problem more carefully, I think the deeper issue is not any single autoscaler.

The issue is that Kubernetes does not have a standard way for a workload to express what kind of capacity it actually wants before the system falls back to Pending Pods as the main signal.

In other words, Kubernetes autoscaling may be missing a **capacity intent layer**.

That is the question I want to reason through in this post.

Over the past few years, Kubernetes autoscaling has accumulated a rich set of components and abstractions: HPA, VPA, KEDA, Cluster Autoscaler, Karpenter, Kueue, DRA, Gateway API Inference Extension, and many others. Each of them solves a real problem, and each can tell a coherent story on its own.

But if we connect these systems end to end, the operational pain my friend described becomes easier to understand.

The common autoscaling path still looks roughly like this:

```text
HPA / workload controller / application autoscaler
  -> create or patch Pods
  -> scheduler writes PodScheduled=False, Reason=Unschedulable into Pod status
  -> Cluster Autoscaler or Karpenter observes Pending Pods
  -> node provisioning happens
  -> scheduler binds Pods to newly available Nodes
```

This model works well enough for many stateless services. A web service replica is usually fungible. A new Pod can handle any request after it becomes ready. Nodes are often treated as mostly interchangeable. If the cluster does not have enough capacity, creating more Pods and letting the node autoscaler react to Pending Pods is a reasonable design.

However, once the workload becomes LLM inference, Ray-based streaming, GPU batch, or stateful data processing, this chain starts to expose a more fundamental limitation.

The issue is not that HPA is too simple, or that Karpenter is not smart enough. The issue is that the downstream layers are often forced to infer too much from a low-semantic signal.

A Pending Pod mainly says:

> Under the current cluster state and this Pod's scheduling constraints, I cannot be scheduled right now.

It does not clearly say:

> Why am I unschedulable? What kind of capacity do I need? How long can I wait? Can I fall back to another GPU class? Can I use Spot? Do I need warm capacity? Can I be interrupted? Is scale-down safe?

In other words, **Pending Pod is a failure-after-the-fact signal**. What many AI and stateful workloads need is closer to a **capacity intent declared before provisioning**.

## HPA's Boundary Is Not Metrics. It Is Semantics.

HPA has a deliberately narrow model. It answers one main question:

> Given some observed metrics, what should the replica count of this workload be?

That abstraction was elegant in the web service era. CPU goes up, add replicas. QPS goes up, add replicas. Once new Pods become ready, a Service or ingress layer can distribute traffic to them. The scaling decision and the routing decision are relatively decoupled. Any healthy replica can usually take any request.

LLM inference is not like that.

Whether an inference replica can serve traffic is not determined merely by whether the Pod is Running. The model may still be loading. The replica may not have warmed up. KV cache locality may matter. Prefill and decode may be separated into different pools. Continuous batching may make GPU utilization look high even when the actual bottleneck is queueing, memory pressure, routing policy, or tail latency.

For LLM serving, the more relevant signals are often things like:

- request queue depth
- TTFT and TPOT percentiles
- batch slot occupancy
- prefill/decode pressure
- KV cache locality
- model load state
- routing-level backpressure
- GPU memory fragmentation or headroom

Of course, HPA can consume custom metrics. That is not the core problem.

The deeper issue is that HPA still outputs a replica count. It does not have a stable API surface to express workload properties such as:

- A new replica needs three minutes of warm-up before receiving critical traffic.
- This model must keep a minimum amount of warm capacity.
- Prefill workers and decode workers have different scaling policies.
- A cold replica without cache locality may increase tail latency in the short term.
- The bottleneck is not replica count, but queueing, routing, or downstream backpressure.
- A newly created Pod depends on a capacity policy that goes beyond fixed Pod-template constraints, such as reservation preference, fallback, capacity type, provisioning latency, or minimum lifetime.

So the limitation of HPA is not that it only looks at CPU. That is a surface-level critique. The deeper limitation is that the HPA API is not where workload capacity semantics live.

It is a replica-count controller, not a capacity intent interface.

## Pending Pod Is a Lossy Interface

The Kubernetes autoscaling chain is beautifully decoupled. Workload controllers create Pods. The scheduler tries to place them. Node autoscalers observe unschedulable Pods and provision more capacity. Components communicate through Kubernetes objects rather than direct RPCs.

That decoupling is one of Kubernetes' strengths.

But the cost is semantic loss across layers.

By the time a node provisioning layer sees the demand, the higher-level intent has often been compressed into a set of Pod-level constraints: resource requests, node selectors, affinity, tolerations, topology spread constraints, PriorityClass, labels, annotations, and maybe a few provider-specific conventions.

Those fields are useful. But they are not enough to reliably reconstruct the workload's intent.

From a set of Pending Pods, the provisioning layer may struggle to know:

- Are these eight Pods a training gang, or eight independent online replicas?
- Must they run in the same zone, same rack, or same placement group?
- Is same-zone a hard requirement or just a preference?
- If H100 capacity is unavailable, can A100 be used as a fallback?
- Can this workload wait ten minutes for reserved capacity, or does it need on-demand capacity immediately?
- Can workers use Spot while the head or driver must use on-demand capacity?
- Should these nodes live for at least twelve hours, or can they be consolidated aggressively?
- Is this Pending state caused by real capacity shortage, quota exhaustion, reservation mismatch, or a bad scheduling constraint?
- During scale-down, which nodes are safe to drain and which would trigger expensive state reconstruction?

Some of this can be encoded today through labels, annotations, NodePools, PriorityClasses, custom CRDs, or provider-specific policies. But that is not the same thing as a stable semantic contract.

The downstream system should not have to guess.

A Pending Pod is a low-semantic-density signal centered around scheduling failure. It is good at telling the system, "Something does not fit." It is much weaker at saying, "This is the kind of future capacity this workload is trying to acquire."

## Inference Exposes the Scale-Up Problem

LLM inference makes the scale-up gap especially obvious.

A normal stateless service often treats a replica as ready once the Pod passes readiness checks. For inference, readiness is only one part of the story. The serving stack may need to load a large model, initialize GPU memory, join a routing layer, warm up kernels, populate cache, and stabilize batching behavior.

Adding a replica can even make things worse in the short term if the routing layer sends traffic to a cold replica too early or if cache locality is destroyed.

This means there are really two layers of intent:

1. **Application-level scaling intent**: routing, queueing, warm-up, readiness, cache locality, prefill/decode split, and request admission.
2. **Capacity-level intent**: GPU class, topology, zone, reservation, capacity type, fallback policy, startup latency, lifetime, and disruption constraints.

Today these two layers are often connected indirectly. The application controller creates Pods. The scheduler fails to place some of them. The node autoscaler reacts. The cloud provider attempts provisioning. The application layer eventually learns whether the new capacity became useful.

That loop works, but it is imprecise and reactive.

For AI serving, the system often needs to know before creating arbitrary Pods:

- Do I need warm standby capacity?
- Should I provision ahead of demand?
- Should I prefer reserved capacity over on-demand?
- Is Spot acceptable for overflow only?
- Can I fall back to a cheaper GPU class?
- Should prefill and decode pools be provisioned differently?
- What is the maximum tolerable provisioning latency?

These are not simply routing decisions. They eventually affect cloud capacity provisioning. But they are also not naturally expressible as plain Pod scheduling constraints.

This is exactly the space where a capacity intent layer would help.

## Streaming Workloads Expose the Scale-Down Problem

Many autoscaling discussions implicitly treat scale-up and scale-down as symmetric:

> Load goes up, add capacity. Load goes down, remove capacity.

For streaming and stateful workloads, that symmetry is false.

Scale-up usually adds helpers. Scale-down removes an execution unit that may hold state, own actors, buffer data, maintain lineage, or participate in an ongoing pipeline. Removing it at the wrong time can trigger reconstruction, spilling, actor restart, backpressure, or end-to-end throughput instability.

Ray-based workloads are a useful example. Application-level actor scaling can understand pipeline bottlenecks because the application control plane knows which actors it created, what they are doing, and whether they are safe to remove. But at the cluster level, node autoscaling typically sees much lower-level signals: resource requests, utilization, idleness, Pod disruption budgets, taints, and node-level constraints.

Ray Data makes this distinction especially concrete. Its application-level execution can scale actor pools based on pipeline pressure and operator bottlenecks, because that logic lives close to the data execution graph. But cluster-level scale-down is necessarily more conservative. A worker node should only be reclaimed after the application has stopped using it and the node becomes idle. In other words, Ray Data can be relatively proactive about application-level scale-up, while cluster-level scale-down is closer to passive reclamation after the workload has made capacity safe to remove.

That distinction is the important part. Safe scale-down is not just a utilization problem. It is an application-state problem.

A node provisioning layer usually cannot know:

- whether a worker currently holds important actor state
- whether deleting a Pod will trigger expensive object reconstruction
- whether low CPU means idle or blocked by downstream backpressure
- whether a streaming pipeline is in a safe drain point
- whether a node is safe to consolidate now or should be preserved

For these workloads, the intent is not merely min/max replicas. The workload needs to express properties like:

- This worker group may scale up automatically, but scale-down must be application-drain-only.
- These nodes may be consolidated; those nodes must not be consolidated.
- These Pods are on the critical path and should not be randomly disrupted by infrastructure-level optimization.
- Spot is acceptable for buffer capacity, but not for baseline capacity.
- Drain must complete before scale-down; if the drain timeout fails, keep the node.

These semantics do not belong entirely to HPA. They also do not belong entirely to kube-scheduler. They need to flow across the boundary between workload control and node provisioning.

## DRA Points in the Right Direction

Dynamic Resource Allocation is not a node autoscaler. It solves a different problem: how workloads declare, select, allocate, and prepare devices that exist in the cluster.

But DRA is interesting beyond GPUs or device plugins. It points toward a more explicit resource model.

Instead of reducing device demand to something like:

```yaml
resources:
  limits:
    nvidia.com/gpu: 1
```

DRA introduces a richer model. DeviceClass describes categories of devices. ResourceClaim expresses a workload's resource request. ResourceSlice represents available device inventory. The scheduler can reason about claims and constraints. The driver participates in preparing and unpreparing resources on the node. Status records the allocation result.

The important shift is this:

**A resource is no longer just an integer. It becomes an object with class, attributes, constraints, allocation result, and lifecycle.**

Node provisioning faces a similar problem, but at a harder layer.

DRA mostly operates over devices and nodes that already exist. Node autoscaling often deals with cloud capacity that does not exist yet. The candidate space is much larger:

```text
instance type
x zone
x capacity type
x reservation
x quota
x price
x startup latency
x topology / placement constraints
```

Placement group is not quite the same type of dimension as instance type or zone. It is more like a supply-side topology constraint. Quota is not inventory. Reservation is not ordinary capacity. Spot has interruption risk. Capacity block has time boundaries. Startup latency matters. Price changes the optimization objective.

Cloud providers also do not expose complete real-time inventory to Kubernetes. A failed CreateFleet or RunInstances call is not an exceptional corner case; it is often part of the solving process. Fallback, reservation selection, quota, placement, capacity type, and startup latency are all provider-side concerns.

So node autoscaling should not copy DRA directly.

But it can borrow DRA's schema philosophy:

- claim-based requests
- class-based policy
- explicit constraints
- provider-specific solving
- allocation status
- lifecycle-aware cleanup
- understandable failure reasons

DRA does not solve cloud capacity provisioning. But it shows why richer resource semantics matter once resources stop being fungible integers.

## The Missing Layer: Capacity Intent

If I had to give the missing piece a name, I would call it **Capacity Intent**, not **Autoscale Policy**.

An autoscale policy usually says:

> Under these metrics, change the replica count this way.

Capacity intent says something different:

> This workload needs a certain kind of capacity, and here is how that capacity may be supplied, substituted, reserved, disrupted, and eventually released.

A hypothetical object might look like this:

```yaml
apiVersion: capacity.k8s.io/v1alpha1
kind: CapacityClaim
metadata:
  name: llama-decode-capacity
spec:
  workloadType: inference

  podSets:
    - name: decode-workers
      count: 8
      templateRef: decode-worker-template

  latency:
    maxProvisioningLatency: 5m
    warmCapacityRequired: true

  topology:
    locality: same-zone
    placementGroup: preferred

  capacityPolicy:
    capacityTypes:
      - reserved
      - on-demand
    spotAllowed: false
    fallback:
      - gpuClass: h100
      - gpuClass: a100

  disruption:
    minLifetime: 12h
    consolidationAllowed: false
    scaleDownPolicy: application-drain-only
```

This object does not have to be called `CapacityClaim`. It may not even need to become a Kubernetes core API. The exact API shape is less important than the missing semantic contract.

The contract should allow the workload to express:

- what type of capacity it needs
- whether the capacity must be warm
- how long provisioning may take
- what topology constraints matter
- which capacity types are acceptable
- whether fallback is allowed
- whether the workload is interruptible
- how scale-down must be coordinated
- how long the capacity should live
- what failure reasons should be reported back

With this layer, the flow becomes different:

```text
Workload declares capacity intent
  -> admission checks quota, fairness, and policy
  -> provisioning layer solves for node capacity
  -> cloud provider attempts allocation
  -> status reports selected capacity or failure reason
  -> scheduler binds Pods to nodes that satisfy the intent
```

The key change is that capacity is declared before the system relies on scheduling failure as the primary signal.

That is a very different model from:

```text
Create Pods first
  -> let them fail scheduling
  -> infer demand from failure
  -> provision nodes
```

For simple services, the current model is fine. For AI and stateful workloads, it often forces too much information to be reconstructed too late.

## ProvisioningRequest Is Already Pointing in This Direction

There are already signs that the Kubernetes ecosystem is moving toward this idea.

Cluster Autoscaler's ProvisioningRequest, and Kueue's integration with it through AdmissionCheck, is one example. The important idea is not that ProvisioningRequest solves every workload category. It does not.

The important idea is that capacity availability can be checked before workload admission instead of being discovered only after Pods become Pending.

That matters.

For batch workloads, this is a natural fit. A job may need a whole group of Pods admitted together. It may not make sense to create all Pods, let them sit Pending, and then hope the autoscaler guesses the group-level intent correctly. By moving capacity checks into admission, the system can reason about the workload as a unit.

But this is still not the full capacity intent layer for inference, streaming, or stateful AI workloads.

It does not fully express:

- warm capacity
- pre-provisioning
- fallback between GPU classes
- reservation lifecycle
- Spot as overflow capacity only
- application-drain-only scale-down
- cache locality
- long-running cluster semantics
- workload-specific disruption constraints

There is also an important caveat: the practical value of this mechanism depends on the autoscaler and cloud provider implementation behind it. Moving the request earlier in the admission path is useful, but the API alone does not guarantee that the provider has enough inventory visibility, supports the relevant capacity classes, can reason about reservations, or can return rich fallback and failure status.

So ProvisioningRequest is best understood as an important signal in the right direction: capacity should become explicit earlier in the control loop.

It is a bridge toward capacity intent, not proof that the full interface already exists.

## Cloud Providers Need to Participate in the Solving Loop

A common platform instinct is to solve everything inside Kubernetes.

For node provisioning, that instinct has limits.

Many critical facts only exist on the cloud provider side:

- which zone currently has a certain GPU instance type
- whether a reservation can satisfy the request
- whether a capacity block is usable
- whether a placement group can still fit the requested nodes
- whether account quota is sufficient
- whether Spot is available and at what risk profile
- how long a certain instance family usually takes to start
- whether fallback to another instance type changes cost and performance too much

Kubernetes should not pretend to have perfect global inventory.

A more realistic role for Kubernetes is to standardize the upper-layer intent, then delegate provider-specific solving to the provisioning implementation. The provider integration can solve within NodePool constraints and translate intent into instance types, zones, capacity types, reservations, NodeClaims, or cloud-specific allocation requests.

Then it should report back through status conditions:

- capacity unavailable
- quota exceeded
- reservation mismatch
- fallback selected
- provisioning in progress
- partially fulfilled
- expired
- interrupted
- drain required
- consolidation blocked

This feedback loop is as important as the initial request.

Without explicit status, the workload layer only sees that Pods are Pending, nodes are missing, or replicas are not useful yet. That is too opaque for complex AI systems.

Karpenter already moves beyond traditional static node groups by dynamically selecting instance types and creating capacity within the boundaries of NodePools and provider-specific NodeClasses. That is a major improvement over older node group-centric models.

But Karpenter still primarily reasons from Pods and their scheduling constraints. The next step is not merely to make node autoscalers more clever at reading Pending Pods. The next step is to give them better intent to work with.

## This Is Not a Universal Autoscaler

There is an easy trap here.

Once we say HPA is not enough and Pending Pod is too lossy, it is tempting to propose a universal autoscaler that manages online services, LLM inference, training, batch, streaming, and stateful data processing through one giant control plane.

I do not think that is the right answer.

These workloads have different scaling semantics.

For ordinary online services, scaling usually means adding fungible replicas.

For LLM inference, scaling is a joint problem across routing, queueing, batching, GPU memory, cache locality, model loading, warm capacity, and capacity provisioning.

For distributed training, the main problem is often not autoscaling at all. It is admission, gang scheduling, topology, failure recovery, checkpointing, quota, and preemption.

For streaming, scale-up and scale-down are not symmetric. Scale-down has to respect application state, drain semantics, and pipeline stability.

Trying to hide all of that behind one universal autoscaler would likely create another overloaded abstraction.

A better direction is to let each workload keep its own control plane:

- HPA can continue to serve ordinary replica scaling.
- KEDA can continue to connect event sources to replica scaling.
- Kueue can continue to handle batch admission and quota.
- DRA can continue to express device allocation.
- Karpenter and cloud-provider integrations can continue to handle node provisioning.
- Inference gateways and serving controllers can continue to own request routing, warm-up, and traffic admission.
- Ray, Spark, Flink, and similar systems can continue to manage their own application-level execution semantics.

The missing piece is not one controller to replace all of them.

The missing piece is a clearer contract between them.

That contract should let the workload say:

> Here is the kind of capacity I need, the constraints under which it is useful, and the lifecycle rules under which it may be changed.

And it should let the provisioning layer say:

> Here is what I can allocate, what fallback I selected, why I failed, and what disruption guarantees I can or cannot provide.

That is what I mean by a capacity intent layer.

## Why This Matters More for AI Infrastructure

This problem existed before AI workloads became popular. Stateful services, distributed data processing, and batch systems have always stretched the Kubernetes scheduling model.

But AI infrastructure makes the issue much more visible.

First, the resources are less fungible. An H100 is not just a bigger CPU. GPU class, memory size, interconnect, topology, MIG configuration, driver version, and placement all matter.

Second, startup cost is higher. Loading a large model, warming kernels, joining a distributed runtime, or restoring state can take meaningful time.

Third, routing and capacity are tightly coupled. A new inference replica is not automatically useful. It must be integrated into request routing, batching, cache, and admission control.

Fourth, cloud capacity is uncertain. GPU supply, reservations, capacity blocks, quota, and Spot availability are all part of the operational reality.

Fifth, disruption is expensive. Removing the wrong node may not just restart a stateless Pod. It may disrupt actors, invalidate cache, trigger object reconstruction, or destabilize a pipeline.

These properties do not fit cleanly into a world where node provisioning mostly learns demand from unschedulable Pods.

AI infrastructure needs a stronger language for capacity.

## Conclusion

This brings me back to my friend's complaint.

The reason Kubernetes autoscaling often feels like it still needs a lot of hand-built DevOps logic is not simply that the autoscalers are immature or that the metrics are not good enough.

A deeper reason is that workload intent is lost as it crosses layers.

HPA sees metrics and outputs replica count.

The scheduler sees Pods and constraints.

Karpenter or Cluster Autoscaler sees Pending Pods.

The cloud provider sees instance provisioning requests.

Each layer has local facts, but there is no standard way to express the workload's capacity properties across the whole path.

Of course, many fields can be encoded today in Pod templates, ResourceClaims, PriorityClasses, topology spread constraints, annotations, custom controllers, NodePools, or provider-specific CRDs. The problem is not that expression is impossible. The problem is that the expression is scattered, implicit, and often reconstructed by convention.

Scattered expression is not a stable interface.

DRA already shows one important lesson on the device side: when resources become complex, an integer request is not enough. The system needs explicit resource attributes, constraints, allocation results, and lifecycle.

Node autoscaling needs a similar evolution, not by copying DRA directly, but by bringing the same philosophy to cloud capacity provisioning.

So the next step for Kubernetes autoscaling may not be to observe Pending Pods more cleverly.

It may be to make the system rely on Pending Pods less.

For simple workloads, scheduling failure can remain a reasonable trigger.

For AI and stateful workloads, capacity should become an explicit intent before every platform team is forced to encode the missing semantics by hand.
