---
layout: post
title: "MultiKueue Solves Dispatch, Not Multi-Cloud GPU Placement"
date: 2026-05-17 16:00:00 -0700
categories: [ml-infrastructure, kubernetes]
tags: [MultiKueue, Kueue, Kubernetes, GPU, Scheduling, ML Infrastructure]
excerpt: "MultiKueue is useful Kubernetes-native plumbing for multi-cluster job dispatch, but it should not be over-sold as a complete multi-cloud GPU placement system."
---

MultiKueue is often presented as a natural answer for multi-cloud GPU scheduling. The framing is understandable: Kueue provides Kubernetes-native queueing, quota, fair sharing, and admission control for batch, HPC, AI/ML, and similar workloads in a Kubernetes cluster. MultiKueue then extends the model across multiple clusters by introducing a manager cluster and worker clusters.

That sounds close to multi-cloud GPU scheduling.

But it is not the same problem.

MultiKueue is useful Kubernetes-native plumbing for dispatching workloads from a manager cluster to worker clusters. That is a real capability. It lets users submit jobs through one control point while executing them on remote Kubernetes clusters. But remote dispatch should not be confused with GPU placement.

For GPU workloads, the hard question is not merely:

> Which cluster should receive this Kubernetes Job?

The harder question is:

> Which resource pool can actually run this workload with the right GPU topology, quota, data locality, cost boundary, tenant policy, and recovery semantics?

Those are different problems.

## What MultiKueue Actually Does

MultiKueue's core model is straightforward. A manager cluster connects to one or more worker clusters. The manager creates and monitors remote Workloads or Jobs on worker clusters and synchronizes status back to the local objects. Worker clusters still behave like standalone Kueue clusters.

By default, MultiKueue uses the `AllAtOnce` dispatching mode: once the manager-side Workload obtains a `QuotaReservation`, the Workload is copied to all available worker clusters. The first worker cluster that admits the remote Workload becomes the selected cluster; the manager then deletes the remote Workloads from the other clusters and creates the corresponding Job in the selected worker cluster.

MultiKueue also supports `Incremental` and `External` dispatcher modes. `Incremental` nominates worker clusters gradually in rounds, while `External` delegates worker-cluster nomination to a custom controller. These modes can narrow or customize the candidate set, but they do not change the core boundary: the manager dispatches after its own quota reservation, and the actual worker-side admission result is resolved after dispatch.

In the right scope, this is useful. If an organization owns several Kubernetes clusters and wants a centralized submission path for Kubernetes-native batch workloads, MultiKueue provides a reasonable mechanism.

But that is still a dispatch mechanism.

It does not automatically solve the full placement problem for GPU workloads across clouds, regions, reservations, pricing models, topology constraints, and data boundaries.

This distinction matters because GPU scheduling is not only about moving a Kubernetes object from one API server to another. It is about deciding where a high-value, topology-sensitive, data-sensitive workload should run.

## The Awkward Middle: Manager-Side Quota Approximation

The most awkward part of MultiKueue's model is the manager-side quota approximation.

The official documentation states that the quota configured in the manager cluster should ideally equal the total quota available across all worker clusters. If the manager quota is significantly lower, worker clusters may remain underutilized. If the manager quota is significantly higher, the manager may dispatch and monitor workloads that are unlikely to be admitted in the worker clusters.

That is the core tension.

If the manager-side quota is conservative, expensive GPUs may sit idle in worker clusters while jobs wait in the manager queue.

If the manager-side quota is optimistic, the manager may send workloads to worker clusters that cannot actually admit them.

Either way, the manager-side quota is not the source of truth. It is a model.

For ordinary batch workloads, this kind of approximation may be acceptable. For GPU workloads, especially expensive multi-GPU or multi-node training jobs, the cost of a weak placement decision is much higher.

A GPU scheduler cannot only ask:

> Is there some quota somewhere behind a worker cluster?

It often needs to know:

```text
Which GPU model is available?
Is it H100, H200, A100, L40S, or something else?
Is it PCIe, SXM, NVL, or a full HGX-style node?
Is the workload single-node or multi-node?
Is the required network available?
Is this capacity reserved, on-demand, or preemptible?
Is the data nearby?
Is cross-cloud or cross-region egress allowed?
Which tenant owns the budget?
What is the queue pressure in the target pool?
Can this job recover if the node disappears?
```

These signals are not just implementation details. They are the placement decision.

## Dispatch Is Not Placement

This is the claim boundary that often gets blurred.

Dispatch means:

```text
Take this workload and submit it to a selected worker cluster.
```

Placement means:

```text
Decide which resource pool should run this workload,
given real capacity, queue pressure, GPU topology, data locality,
tenant policy, cost, priority, and failure recovery requirements.
```

MultiKueue helps with the first problem.

It does not, by itself, own the second problem.

This difference is especially important for GPU infrastructure because GPU resources are not fungible in the same way as generic CPU capacity. Eight H100s in one cloud region are not necessarily equivalent to eight H100s in another region or provider. The instance shape, local GPU interconnect, inter-node network, reservation status, storage path, data locality, and operational boundary all matter.

Treating all of that as a remote-cluster dispatch problem hides the hardest part of the system behind a Kubernetes-native abstraction.

## Kueue's Model Is Powerful, But Its Strength Has a Boundary

Kueue's internal model is powerful inside a Kubernetes control domain.

A `ClusterQueue` governs a resource pool and defines quotas, usage limits, and fair-sharing rules across multiple ClusterQueues. It can manage resources such as pods, CPU, memory, and hardware accelerators.

A `ResourceFlavor` represents resource variations and associates them with nodes through labels, taints, and tolerations.

A `Cohort` lets ClusterQueues share quota with each other, allowing unused quota to be borrowed within the same sharing structure.

This is a good model for Kubernetes-native quota and admission control.

The problem starts when this model is over-generalized into enterprise multi-cloud GPU scheduling.

A department is a business concept.

A cloud GPU pool is a technical and economic entitlement.

A GPU placement decision is a combination of capacity, topology, cost, policy, and data constraints.

Those concepts do not always map cleanly into a single queue/cohort/flavor graph.

Inside a stable Kubernetes GPU pool, Kueue's model can be a strong fit.

Across dynamic cloud GPU capacity, the model becomes much harder to reason about.

## The Fork in the Road

MultiKueue faces a fundamental fork.

If it does not know real worker-side quota, queue pressure, GPU topology, data locality, tenant budget, and cost constraints before dispatch, then it can only make a best-effort remote submission.

If it does know all of those signals before dispatch, then the real placement decision is already being made somewhere else. At that point, MultiKueue becomes one execution path, not the top-level scheduling abstraction.

This is why the manager-side quota approximation is not a minor detail. It reveals the deeper issue: MultiKueue is not designed to be the full source of truth for multi-cloud GPU placement.

It can move jobs across clusters.

But moving jobs is not the same as deciding where expensive GPU workloads should run.

## Where MultiKueue Fits

The point is not that MultiKueue is useless.

It is useful for what it is:

```text
Kubernetes-native multi-cluster job dispatch.
```

It is a reasonable fit when:

```text
The organization owns multiple Kubernetes clusters.
The clusters are relatively stable.
The workloads are Kubernetes-native.
The main goal is centralized submission and remote execution.
The placement policy is simple enough to be approximated at the manager.
```

But it should not be over-sold as:

```text
A complete enterprise multi-cloud GPU scheduler.
```

That broader problem requires placement decisions based on signals that go beyond remote Kubernetes object creation.

## Conclusion

MultiKueue solves dispatch, not multi-cloud GPU placement.

That distinction matters.

For GPU workloads, the hard part is not simply sending a Job to another cluster. The hard part is deciding which resource pool should run the workload in the first place, given GPU topology, quota, queue pressure, data locality, tenant policy, cost, and recovery requirements.

MultiKueue is useful Kubernetes-native plumbing. But treating it as the center of a multi-cloud GPU scheduling architecture risks overestimating what remote dispatch can solve.

The right claim boundary is simple:

> MultiKueue can help dispatch Kubernetes workloads across clusters.  
> It should not be confused with a full multi-cloud GPU placement system.

## References

- [Kueue overview](https://kueue.sigs.k8s.io/)
- [MultiKueue concept](https://kueue.sigs.k8s.io/docs/concepts/multikueue/)
- [ClusterQueue concept](https://kueue.sigs.k8s.io/docs/concepts/cluster_queue/)
- [ResourceFlavor concept](https://kueue.sigs.k8s.io/docs/concepts/resource_flavor/)
- [Cohort concept](https://kueue.sigs.k8s.io/docs/concepts/cohort/)
