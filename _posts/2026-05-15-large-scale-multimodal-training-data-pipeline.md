---
layout: post
title: "Large-Scale Multimodal Training Data Pipelines: Pattern C, Pattern B, and the Boundary Between Them"
date: 2026-05-15
categories: [ml-infrastructure, data-pipeline, distributed-systems]
tags: [multimodal, training-data, ray-data, webdataset, lance, vortex]
---

This article is not a benchmark report, and it is not a final architecture proposal for any specific project. It is closer to a personal engineering note. I am trying to reason about large-scale multimodal training data pipelines through a few dimensions: storage layout, execution model, cache economics, and data lifecycle.

Many of the views here come from public case studies, source-code reading, and architecture reasoning rather than complete controlled benchmarks. I will try to separate relatively stable engineering facts from observations, inferences, and open questions.

---

## 1. The problem is not just tool selection

Discussions about multimodal training data pipelines often collapse into tool comparisons:

- Ray Data vs. DALI
- WebDataset vs. Parquet
- Lance vs. Vortex
- tf.data vs. PyTorch DataLoader
- local SSD cache vs. object store streaming

These comparisons are useful, but they can also be misleading if the tools are treated as if they sit at the same layer.

My current view is that the core architecture question is not simply:

> Should we use Ray Data or WebDataset?

A better framing is:

> As data moves from raw recording to curation, filtering, alignment, sampling, and training, which stages should remain dynamic, and which stages should be materialized into a stable high-throughput training format?

That boundary matters more than any individual tool choice.

---

## 2. The dimensions I use to reason about the problem

To avoid comparing tools at the wrong layer, I find it useful to separate the pipeline into several dimensions.

### 2.1 Storage layer: how data is physically organized

One direction is a **pre-aligned shard** layout:

- TFRecord
- Mosaic MDS
- WebDataset tar
- other training-sample-oriented shards

In this layout, the data needed for a training sample is mostly aligned ahead of time. During training, each rank reads its assigned shards, performs decode or lightweight transforms, and feeds batches to the model.

Another direction is a **metadata + blob** layout:

- metadata in Parquet, Lance, Vortex, or another columnar format
- video, image, lidar, embedding, or other large objects stored as independent blobs
- runtime association through episode id, frame id, timestamp, sensor id, or other keys

The first direction optimizes the final training loop. The second direction optimizes flexibility, selective access, and dataset evolution.

### 2.2 Execution layer: how data is processed and delivered to GPUs

One direction is **per-rank iterable loading**:

- each training rank has its own data loader
- each rank reads assigned shards
- ranks usually do not exchange data
- shuffle happens at shard order, buffer, or worker level

Examples include tf.data, PyTorch IterableDataset, Mosaic StreamingDataset, WebDataset loaders, and Grain-style loaders.

Another direction is a **distributed streaming DAG**:

- read, decode, filter, join, map, batch, and sampling stages are represented as a distributed execution graph
- CPU workers or actors can scale independently
- data can move across nodes
- heterogeneous preprocessing can be expressed naturally

Ray Data, Dask, and Spark-like systems are closer to this direction. They are not just dataloaders; they are distributed data execution engines.

### 2.3 Acceleration layer: decode and IO path

DALI, nvJPEG, NVDEC, and GPUDirect Storage are better understood as acceleration layers rather than as full storage or execution patterns.

They answer questions like:

- should decode happen on CPU or GPU?
- can image or video decode use hardware acceleration?
- how many copies happen between storage, host memory, and GPU memory?
- can the IO path be shortened?

These capabilities can be combined with different execution models. They are important, but they do not by themselves decide the pipeline lifecycle boundary.

### 2.4 Cache layer: where reuse happens

Cache is often discussed too casually. In training data pipelines, several very different cache semantics may exist:

- pre-sharded files on local SSD
- dataloader worker buffers
- distributed object-store materialized blocks
- spilled objects on local disk
- application-level rolling shard cache
- metadata or index cache

The important properties are cache granularity, lifecycle, capacity model, invalidation behavior, and cross-epoch reuse. Saying that a system “has cache” is not precise enough.

---

## 3. Two common patterns: Pattern C and Pattern B

For discussion, I use two rough patterns.

### Pattern C: pre-shard + per-rank iterable

Pattern C looks like this:

```text
pre-aligned dataset shards
  → per-rank iterable loader
  → local shuffle / decode / transform
  → training step
```

This is the classic high-throughput training-loop path. When the dataset is stable, preprocessing is predictable, and the training topology is relatively fixed, Pattern C is extremely strong.

Its strengths are straightforward:

- short data path
- simple rank-level ownership
- good locality
- simple mental model
- natural compatibility with local SSD cache
- high throughput when shards are precomputed well

Many successful large-scale training systems use this idea in some form. Text pretraining, image classification, and some multimodal pretraining pipelines can all benefit from this layout when the assumptions hold.

### Pattern B: distributed streaming DAG

Pattern B looks like this:

```text
metadata + blob storage
  → distributed read / filter / join / decode / transform
  → dynamic batching or sampling
  → training or dataset materialization
```

Pattern B trades some simplicity and peak single-node throughput for flexibility. It is more natural when the pipeline includes:

- complex preprocessing
- multi-source joins
- heterogeneous CPU/GPU stages
- frequent dataset evolution
- dynamic filtering or resampling
- multiple training stages
- shared infrastructure across different workloads

The key value of Pattern B is not that it is always faster. Often it is not. Its value is that it keeps more of the data lifecycle dynamic.

---

## 4. Pattern C is very strong when its assumptions hold

Pattern C can be close to ideal for the final training loop.

If data has already been aligned and materialized into training-ready shards, the runtime loader does not need to solve many hard problems. It can mostly stream through shards, perform local shuffle, decode, and batch.

This has several advantages.

First, the data path is short. There is no need for runtime joins or distributed data movement between ranks.

Second, local cache semantics are clear. If shards are placed on local SSD, multiple epochs can reuse the same physical files.

Third, the training loop is predictable. Rank-to-shard assignment, dataloader worker behavior, and failure modes are relatively easy to reason about.

Fourth, hardware acceleration can work well when the data layout is compatible. Static image or video decode pipelines can benefit from GPU decode and optimized buffer management.

For stable training workloads, Pattern C is hard to beat.

---

## 5. Where Pattern C becomes fragile

The challenge is that Pattern C depends on a set of static assumptions.

Those assumptions are not wrong. They are the design contract. The question is whether a workload satisfies them.

### 5.1 Stable rank topology

Pattern C works best when the number of ranks and their placement are stable. If the system needs elastic scaling, frequent preemption recovery, spot instances, or heterogeneous node types, shard assignment and cache locality become harder to maintain.

### 5.2 Dataset fits the cache model

Local shard cache is powerful when the active dataset fits into aggregate local SSD or when the access pattern is stable enough to reuse cached shards.

If the active working set is much larger than the cache, and each epoch or training phase reshuffles globally, cache hit rate becomes bounded by the ratio between cache capacity and active working set. The cache may still help, but the economics become less favorable.

### 5.3 Preprocessing is stable

Pattern C assumes that much of the expensive alignment and preprocessing can be done ahead of time. This is reasonable for stable data releases, but less so when teams frequently change filtering rules, sampling policies, sensor alignment logic, or training stage definitions.

### 5.4 Workload is relatively uniform

When multiple workloads share the same data platform but require different preprocessing, different sampling, different modalities, and different resource profiles, a purely pre-sharded approach may fragment into many specialized pipelines.

In these cases, Pattern C may still be the right final training format, but it may not be the right abstraction for the whole data lifecycle.

---

## 6. Pattern B buys dynamicity, not free performance

Pattern B is attractive because it keeps computation dynamic for longer.

It can express:

- CPU-heavy preprocessing
- multi-source joins
- filtering and re-filtering
- embedding or caption generation
- heterogeneous actor pools
- per-stage resource allocation
- incremental dataset assembly
- dynamic sampling and resampling

This is especially useful for video-heavy multimodal workloads, robotics datasets, autonomous driving datasets, and large-scale curation pipelines.

But Pattern B has real costs.

Distributed data movement is harder to optimize than per-rank sequential reading. Object-store pressure, block sizing, backpressure, actor-pool scaling, locality, and shuffle strategy all matter. Defaults can make the pipeline run, but high performance usually requires profiling and tuning.

My current view is not that Pattern B is better than Pattern C. A more careful statement is:

> Pattern B is a good default for dynamic curation and data assembly. Pattern C is often a better target for a stable high-throughput training loop. The architecture question is where to place the materialization boundary.

---

## 7. Cache economics should be treated as a first-class design axis

Cache is one of the main reasons this boundary is difficult.

Pattern C has a very strong cache story: pre-sharded files can live on local SSD. The unit of reuse is explicit and predictable.

Pattern B can also reuse work, but the semantics are different. A distributed data engine may materialize blocks in an object store, spill to local disk, or allow an application to implement a rolling materialization strategy.

This can reduce repeated object-store reads, but it does not fully reproduce the deterministic local-shard cache semantics of Pattern C.

So I would not say that Pattern B “eliminates” Pattern C’s cache advantage. A more accurate statement is:

> Pattern B weakens the exclusivity of Pattern C’s cache story, but cache still needs to be designed explicitly.

For large multimodal workloads, I think cache design should be discussed independently from both storage format and execution engine.

---

## 8. The storage frontier: Parquet + blobs, Lance, and Vortex

For Pattern B-style systems, the storage layer often becomes a metadata + blob layout.

A common pattern is:

```text
metadata/
  episodes.parquet
  frames.parquet
  tasks.parquet
  stats.json

videos/
  camera_front/...
  camera_left/...
  camera_right/...
```

This is flexible and easy to integrate with distributed processing systems, but it creates a few challenges:

- many metadata files
- runtime joins between metadata and blobs
- object-store listing overhead
- lifecycle management for blob files
- versioning and cleanup of derived datasets

This is where formats such as Lance and Vortex become interesting.

### 8.1 Lance

My current reading is that Lance is most interesting for video-heavy multimodal workloads not because it magically improves codec-level video decode, but because it can make dataset-level governance more coherent.

For large opaque video blobs, the dominant costs are often still video IO, seek behavior, decode, and sampling strategy. Lance does not change the video codec itself.

However, Lance may help with:

- unified metadata and blob descriptors
- dataset versioning
- manifest-level consistency
- blob lifecycle management
- base-path relocation
- selective access through metadata

So I would frame Lance’s value this way:

> For video-heavy workloads, Lance is less about replacing the video codec and more about managing the dataset as a coherent versioned object.

That can still be very valuable.

### 8.2 Vortex

Vortex appears more focused on columnar access, encoding, and random access for tabular or metadata-heavy data.

That is relevant because metadata access can matter a lot in large-scale dataset filtering and sampling. But for already-compressed video blobs, a columnar format does not automatically solve codec-level decode or GOP-level seek problems.

So my current framing is:

> Vortex may be very interesting for metadata-heavy ML tables and feature-store-like access patterns. For compressed video blobs, its advantage is less obvious unless it is paired with a broader blob and lifecycle strategy.

### 8.3 Parquet + independent blobs

Plain Parquet plus independent video files is still a strong baseline.

It is easy to understand, easy to integrate, and compatible with many tools. The downside is that lifecycle management, cleanup, versioning, and metadata/blob consistency become application responsibilities.

This may be acceptable for release-once datasets. It becomes harder when data evolves frequently.

---

## 9. Recording format is not training format

A common question in robotics and autonomous driving is why training pipelines do not simply train directly on recording formats such as rosbag or MCAP.

The reason is that recording formats and training formats optimize different invariants.

Recording formats optimize for:

- append-safe logging
- multiple producers writing independently
- time-indexed replay
- message-level atomicity
- robustness to dropped frames, late messages, clock skew, and crashes

Training formats optimize for:

- sample-level random access
- modality-specific filtering
- aligned clips or frames
- efficient decode
- batching and shuffling
- repeated access across many epochs or experiments

This is a lifecycle conversion:

```text
raw recording
  → curation / alignment / filtering
  → training dataset
```

Video plays a special role here. It is not just a container; it also exploits temporal continuity through inter-frame compression. But that compression only works well after curation has established a stronger invariant: frames belong to a coherent, ordered, seekable clip.

In other words, the compression benefit is not just “because video codecs are good.” It is because curation changes the semantic invariant of the data.

That is why training format and recording format should not be treated as the same design problem.

---

## 10. A more useful architecture framing: where is the B-to-C boundary?

The most useful way I currently think about this space is not “Pattern B vs. Pattern C.”

It is:

```text
raw recording
  → dynamic curation
  → dynamic filtering / joining / sampling
  → dataset materialization
  → stable high-throughput training
```

The hard architecture question is where to place the boundary between dynamic execution and materialized training format.

Move the boundary earlier, and the training loop becomes simpler and faster. But every dataset change requires more materialization work.

Move the boundary later, and the system becomes more flexible. But the training loop inherits more distributed execution complexity.

The right boundary depends on:

- data volatility
- preprocessing complexity
- active working set size
- cache capacity
- rank stability
- training stage diversity
- cost of rematerialization
- need for governance and versioning

This is why I do not think there is a single universal answer.

---

## 11. My current mental model

Here is the simplified version of my current view.

Pattern C is excellent when:

- the dataset is stable
- training samples can be pre-aligned
- rank topology is predictable
- local cache can be reused
- preprocessing is mostly fixed
- training-loop throughput is the dominant concern

Pattern B is attractive when:

- data evolves frequently
- preprocessing is complex
- multiple workloads share the same data platform
- CPU and GPU stages need different scaling behavior
- filtering, joining, or sampling changes often
- curation is a major part of the pipeline

Most large multimodal systems probably need both.

A good architecture should not ask only which pattern wins. It should ask:

> Which parts of the data lifecycle should remain dynamic, and which parts should be materialized into a stable training format?

That is the question I find most useful.

---

## 12. What I would still want to benchmark

A lot of the reasoning above still needs workload-specific measurement.

The questions I would want to benchmark include:

- At what dataset scale does metadata-layer small-file IO become a real bottleneck?
- How much does Lance help with versioning, cleanup, and metadata/blob consistency in practice?
- Does Vortex provide meaningful benefit for metadata-heavy training data access patterns?
- What is the cost of global shuffle versus shard-level shuffle plus local buffer shuffle?
- How effective is distributed object-store materialization compared with local SSD shard cache?
- Where is the best materialization boundary for a specific multimodal training workload?
- How often does the dataset change enough to invalidate pre-sharded training formats?

Until those are measured, I would treat this article as a framework for thinking, not as a final answer.

---

## Closing thought

For large-scale multimodal training data pipelines, the most important design decision may not be the storage format or the data engine alone.

The more fundamental decision is:

> When should data remain dynamic, and when should it become a stable training artifact?

That boundary determines how much flexibility the system keeps, how much throughput the training loop gets, and how expensive dataset evolution becomes.

That is the part I am still trying to reason about.
