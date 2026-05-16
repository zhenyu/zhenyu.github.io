---
layout: post
title: "Design Patterns for Large-Scale Multimodal Training Data Pipelines"
date: 2026-05-15
categories: [ml-infrastructure, data-pipeline, distributed-systems]
tags: [multimodal, training-data, data-pipeline, ray-data, webdataset, lance, vortex]
---

I wrote this mostly to clarify my own thinking.

When I look at large-scale multimodal training data systems, I keep seeing the same confusion: people compare tools before agreeing on which lifecycle stage they are optimizing. A data curation system, a storage layout, a training-time `DataLoader`, and a cache strategy are related, but they are not the same layer.

So this post is not a benchmark report, and it is not a universal architecture recommendation. It is a personal reasoning framework: how I currently think about the design space, what patterns seem to appear repeatedly, and where I think the real trade-offs are.

The core question I keep coming back to is:

> At which point in the data lifecycle should a pipeline stop being dynamic and become a stable training artifact?

That boundary determines how much flexibility the system keeps, how much throughput the training loop can get, and how expensive dataset evolution becomes.

---

## 1. The real problem: data lifecycle, not just data loading

A multimodal training pipeline usually has more stages than the final `DataLoader`.

A simplified lifecycle looks like this:

```text
raw recording
  -> curation / filtering / alignment
  -> metadata and blob organization
  -> sampling / joining / transformation
  -> dataset materialization
  -> high-throughput training
```

Different stages optimize for different things.

The recording stage cares about append safety, durability, and replay.  
The curation stage cares about dynamic filtering, joins, decoding, enrichment, and versioning.  
The training stage cares about throughput, shuffle quality, deterministic rank behavior, cache reuse, and failure recovery.

The mistake is to force one abstraction to optimize all of these stages equally well.

That is why I find it useful to reason in terms of **patterns**, rather than individual tools.

---

## 2. A few design dimensions

Before comparing patterns, I separate the problem into a few dimensions.

### 2.1 Storage layout

There are two broad directions.

The first is a **training-sample-oriented layout**:

```text
shard-000.tar / shard-000.mds / shard-000.tfrecord
shard-001.tar / shard-001.mds / shard-001.tfrecord
...
```

Samples are mostly aligned before training. The training loop streams through shards.

The second is a **metadata + blob layout**:

```text
metadata/
  episodes.parquet
  frames.parquet
  tasks.parquet

videos/
  camera_front/...
  camera_left/...
  camera_right/...

other_blobs/
  lidar/...
  embeddings/...
```

Metadata is queryable. Large objects stay as separate blobs. Alignment and sampling can happen later.

### 2.2 Execution model

One direction is **per-rank iterable execution**:

```text
rank 0 -> assigned shards -> local workers -> batches
rank 1 -> assigned shards -> local workers -> batches
rank 2 -> assigned shards -> local workers -> batches
```

Each rank mostly reads independently. This keeps the training loop simple.

Another direction is **distributed data execution**:

```text
read -> filter -> join -> decode -> transform -> batch
```

The pipeline is a distributed DAG. CPU and GPU stages may scale differently. Data may move across nodes.

### 2.3 Cache semantics

Cache is not one thing.

A pipeline may cache:

- pre-sharded files on local SSD
- decoded or partially decoded blocks in a distributed object store
- metadata indices
- rolling windows of materialized shards
- temporary spill files
- dataloader worker buffers

These have different lifetimes, eviction behavior, and failure semantics. A good architecture should discuss cache explicitly rather than treating it as a detail hidden behind the loader.

### 2.4 Dataset evolution

Some datasets are release-once artifacts. Others change constantly:

- filtering rules change
- bad data is removed
- new sensors or modalities are added
- sampling weights change
- curation models are updated
- training stages use different views of the same underlying data

The faster the dataset evolves, the more expensive early materialization becomes.

---

## 3. Three useful patterns

I currently find it helpful to classify common systems into three rough patterns.

These are not strict categories. Real systems often combine them, and the same organization may use different patterns at different lifecycle stages.

### Pattern A: direct random-access loader over metadata and blobs

This is the simplest flexible baseline.

```text
metadata table + blob files
  -> custom Dataset / DataLoader
  -> per-sample fetch / decode / transform
  -> batch
```

Examples:

- a PyTorch `Dataset` reading Parquet rows and video files
- a robotics dataset loader reading episode metadata and MP4 clips
- a custom autonomous-driving loader joining sensor files at runtime

This pattern is attractive because it is easy to start with. It preserves flexibility and avoids expensive pre-sharding.

But at scale, it can become difficult:

- many small metadata reads
- many object-store requests
- runtime joins in the training path
- limited global optimization
- cache behavior hidden inside application code
- hard-to-control per-rank skew

Pattern A is often a good research or early-stage format. It is rarely the final answer for very large, high-throughput training.

### Pattern B: distributed streaming DAG

Pattern B keeps the data pipeline dynamic, but moves the work into a distributed execution layer.

```text
metadata + blobs
  -> distributed read / filter / join / decode / transform
  -> distributed shuffle or block-level sampling
  -> batches or materialized dataset
```

Examples of tools in this family include Ray Data, Dask, Spark-like systems, and custom distributed curation pipelines.

This pattern is useful when the pipeline contains:

- large-scale filtering
- video decoding and clipping
- embedding generation
- captioning or labeling
- multi-source joins
- CPU-heavy geometric transforms
- dataset version construction
- dynamic sampling logic

Pattern B is not automatically faster than a good pre-sharded training loader. Its value is flexibility and resource decoupling.

It can scale CPU-heavy curation separately from GPU-heavy training. It can express heterogeneous stages more naturally. It can delay materialization until the dataset view is stable enough.

The cost is operational complexity:

- block sizing matters
- shuffle strategy matters
- object-store pressure matters
- actor or worker pool sizing matters
- locality is harder to guarantee
- default settings may run but not be optimal

Pattern B buys dynamicity. It does not buy free performance.

### Pattern C: pre-sharded training format with per-rank iterable loading

Pattern C materializes the dataset into a training-friendly format before the final training loop.

```text
curated training shards
  -> per-rank iterable loader
  -> local shuffle / decode / transform
  -> training step
```

Examples:

- WebDataset tar shards
- Mosaic MDS
- TFRecord shards
- Grain/tf.data-style sharded iterable pipelines
- precomputed video/image shards consumed by PyTorch DataLoader

This pattern is very strong when the dataset is stable.

Its advantages are clear:

- short training data path
- predictable rank-to-shard assignment
- good sequential IO
- natural local SSD cache
- simpler failure model
- high throughput when samples are well aligned
- fewer runtime joins in the training path

This is often the best final training-loop pattern.

But it depends on assumptions:

- training samples can be pre-aligned
- preprocessing is stable enough
- rematerialization is not too expensive
- rank topology is predictable
- cache can be reused across epochs or runs
- one shard layout can serve the workload well

When these assumptions hold, Pattern C is hard to beat. When they do not, the cost of pre-sharding can dominate.

---

## 4. Comparing the patterns

A simplified comparison looks like this:

| Dimension | Pattern A: direct random access | Pattern B: distributed streaming DAG | Pattern C: pre-sharded iterable |
|---|---|---|---|
| Startup cost | Low | Medium | High |
| Flexibility | High | High | Low to medium |
| Training-loop throughput | Low to medium | Medium | High |
| Runtime joins | Common | Explicit in DAG | Mostly avoided |
| Cache semantics | Application-defined | Object/block/window-level | File/shard-level |
| Resource scaling | Per training job | Per pipeline stage | Mostly tied to training ranks |
| Dataset evolution | Easy | Easy to medium | Expensive if frequent |
| Operational complexity | Hidden in app code | High but explicit | Medium and predictable |
| Best use case | early experimentation | curation and dynamic assembly | stable high-throughput training |

This table is intentionally rough. The point is not that one pattern is universally better. The point is that each pattern has a different design contract.

---

## 5. When Pattern A is enough

Pattern A is often a good starting point.

If the dataset is small enough, the number of modalities is limited, and training throughput is not yet the bottleneck, a direct loader over metadata and blobs may be the simplest solution.

This is especially true during early dataset exploration:

- validating data schema
- debugging alignment
- inspecting episodes
- testing new filtering logic
- iterating on model input format

The danger is that Pattern A can survive too long. Once the same runtime joins and blob fetches happen repeatedly in every training run, the system may be paying curation cost inside the training loop.

A useful question is:

> Are we doing one-time data assembly work repeatedly during training?

If yes, Pattern A may need to evolve into Pattern B or C.

---

## 6. When Pattern B is the right center of gravity

Pattern B becomes attractive when the data view is still changing.

For example:

- the team is still changing filtering rules
- multiple training stages need different sample views
- preprocessing includes CPU-heavy sensor alignment
- video clips must be decoded, split, embedded, captioned, or re-encoded
- metadata and blob lifecycle need to be versioned together
- workloads require different CPU/GPU ratios
- the same data platform serves many teams or training jobs

In these cases, making the training format too early can create unnecessary churn. Every change may require rematerializing a large dataset.

Pattern B lets the system keep more of the pipeline dynamic until the dataset view becomes stable.

But Pattern B should not be mistaken for a magical training loader. If every training step depends on a complex distributed DAG, the training loop inherits the complexity of the data system.

That may be acceptable for some workloads. For many workloads, Pattern B is better used to produce a stable artifact that Pattern C can consume.

---

## 7. When Pattern C is the right target

Pattern C is the natural target once the dataset view stabilizes.

It is especially strong when:

- the training loop dominates cost
- data samples can be pre-aligned
- the same dataset will be reused many times
- cache reuse matters
- shuffle requirements can be approximated by shard-level and buffer-level methods
- training rank topology is stable enough
- runtime joins are unnecessary or expensive

This is why many high-throughput training systems eventually converge toward some kind of pre-sharded format.

The interesting question is not whether Pattern C is fast. It is.

The interesting question is:

> Is the dataset stable enough to justify materializing it into Pattern C?

If the answer is yes, Pattern C is often the cleanest final training-loop design.

If the answer is no, forcing Pattern C too early can create a heavy rematerialization tax.

---

## 8. Cache economics

Cache is one of the main reasons the boundary is hard.

Pattern C has the cleanest cache story. If training shards live on local SSD, the unit of reuse is explicit. The training job can reuse physical files across epochs or runs.

Pattern B can also reuse work, but through different mechanisms:

- materialized distributed blocks
- object-store-backed cache
- spill to local disk
- rolling shard materialization
- application-level cache windows

These mechanisms can reduce repeated object-store reads, but they are not equivalent to deterministic local shard cache.

So I would not say that Pattern B eliminates Pattern C's cache advantage. A better statement is:

> Pattern B can reduce the exclusivity of Pattern C's cache story, but cache still needs to be designed explicitly.

A useful way to reason about cache is:

```text
cache hit rate is bounded by active working set, cache capacity, sampling distribution, and invalidation frequency
```

If the active working set is far larger than aggregate cache and global reshuffling changes access patterns every epoch, cache reuse may be limited.

If the same curated shards are reused across many runs, Pattern C cache can be extremely effective.

---

## 9. An open storage-layer subquestion: Parquet + blobs, Lance, and Vortex

This section is intentionally secondary to the main argument.

The main question of this post is where to place the materialization boundary in a multimodal training data lifecycle. Storage formats such as Parquet, Lance, and Vortex matter after we decide that part of the pipeline should remain in a metadata + blob layout.

So I treat this section as an open storage-layer subquestion, not as the core thesis of the post.

### 9.1 Parquet + independent blobs

Parquet plus independent blob files is a strong baseline for metadata + blob datasets.

It is easy to inspect, easy to integrate with data tools, and easy to evolve early on. It works well when the team is still exploring schemas and sampling logic.

The downside is that consistency and lifecycle management become application responsibilities:

- which blob files are still referenced?
- which metadata version points to which blob version?
- how are old versions cleaned up?
- how are paths relocated across buckets?
- how are runtime joins optimized?

For stable or small datasets, this may be fine. For frequently evolving datasets, it becomes operational work.

### 9.2 Lance

My current hypothesis is that Lance's value in video-heavy multimodal workloads may be less about codec-level throughput and more about dataset governance.

For large opaque video blobs, I would not expect Lance to magically improve video decoding. The video still has to be read, sought, and decoded. The possible value is elsewhere:

- dataset-level versioning
- manifest consistency
- blob lifecycle management
- metadata/blob coordination
- selective access through metadata
- path relocation and governance

This could be valuable even without a codec-level performance win. But I would not treat it as a settled conclusion without workload-specific measurement.

So my current framing is:

> Lance may be most interesting when metadata/blob lifecycle is becoming a system problem, not merely when raw video decode throughput is the bottleneck.

### 9.3 Vortex

My tentative read is that Vortex is more naturally relevant to metadata-heavy or feature-table-like access patterns.

That may matter for large-scale filtering, sampling, and feature access. But for already-compressed video blobs, a columnar format does not by itself solve GOP-level seek or video decode.

Whether Vortex matters for a video-heavy training pipeline depends on how much of the end-to-end cost is actually in metadata access and sampling, rather than blob IO and decode.

So I would frame Vortex as a candidate to watch for the metadata-heavy parts of the pipeline, not as a direct replacement for video-aware storage and decode design.

### 9.4 WebDataset / MDS / TFRecord-style shards

These formats are closer to the Pattern C side of the boundary.

They are excellent when samples are already curated and aligned. They reduce runtime joining and make the training loop more predictable.

Their cost is that they encode a more fixed view of the dataset.

This is why I think storage-format discussion should be downstream of the lifecycle question. First decide what should remain dynamic and what should be materialized. Then choose the format that matches that role.

---

## 10. Recording format is not training format

Robotics and autonomous-driving systems often start with recording formats such as rosbag or MCAP.

These formats optimize for a different lifecycle stage.

Recording formats care about:

- append-safe logging
- multiple producers writing independently
- time-indexed replay
- message-level atomicity
- robustness to dropped frames, late messages, clock skew, and crashes

Training formats care about:

- sample-level random access
- modality-specific filtering
- aligned clips or frames
- efficient decode
- batching and shuffling
- repeated access across many epochs or experiments

This is an invariant conversion:

```text
raw recording invariant:
  message-level atomic, append-safe, time-indexed

training invariant:
  aligned, sampleable, seekable, batchable
```

Video is a good example. Video compression works well because frames are organized as coherent temporal sequences. That assumption is usually not safe at raw recording time. It becomes safe only after curation has established the right clip-level invariant.

So the benefit of video is not just that codecs are good. It is that curation changes the semantic structure of the data enough for video compression and seek behavior to become useful.

---

## 11. A better way to frame the architecture decision

Instead of asking:

> Which tool should we use?

I think the better sequence is:

1. What is the current data lifecycle?
2. Which stages are still changing?
3. Which stages are repeated often enough to justify materialization?
4. What is the active working set?
5. What cache semantics do we need?
6. How stable is the training topology?
7. How expensive is rematerialization?
8. How much governance do metadata and blobs need?

Only then should we choose tools.

A rough decision rule:

- Start with Pattern A if the dataset is small or still being understood.
- Move to Pattern B when curation, filtering, joining, or preprocessing becomes large and dynamic.
- Materialize into Pattern C when the dataset view is stable enough and training-loop throughput dominates.

Many production systems will use all three at different stages.

---

## 12. My current mental model

The core architecture decision is the **materialization boundary**.

Move the boundary earlier:

```text
curation -> pre-sharded training dataset -> simple training loop
```

You get a faster, simpler training loop, but dataset evolution becomes more expensive.

Move the boundary later:

```text
curation + filtering + sampling remain dynamic -> training consumes dynamic stream
```

You get more flexibility, but the training loop inherits more distributed data complexity.

There is no universal best point. The right boundary depends on:

- dataset volatility
- preprocessing complexity
- number of workloads
- active working set size
- cache capacity
- object-store economics
- training topology stability
- governance and versioning needs
- cost of rebuilding training shards

That is why I think "Ray Data vs. WebDataset" is the wrong top-level framing.

The better framing is:

> Which parts of the pipeline should remain dynamic, and which parts should become stable training artifacts?

---

## 13. What I would still want to benchmark

This post is mostly a reasoning framework. To turn it into an architecture decision for a specific workload, I would want measurements.

Some questions I would benchmark:

- How much time is spent in metadata access, blob fetch, decode, transform, and batching?
- How expensive is global shuffle compared with shard-level shuffle plus local buffer shuffle?
- How effective is local SSD shard cache for the active working set?
- How effective is distributed block materialization or rolling-window cache?
- How often does the dataset change enough to invalidate pre-sharded artifacts?
- How much operational complexity does metadata/blob lifecycle create?
- Does Lance reduce governance complexity enough to justify migration?
- Does Vortex help metadata-heavy sampling or filtering enough to matter?

Without those numbers, the safest conclusion is not "Pattern B wins" or "Pattern C wins."

The safer conclusion is:

> Pick the materialization boundary based on workload volatility, cache economics, and training-loop throughput requirements.

---

## Closing thought

A large-scale multimodal training data pipeline is not just a loader.

It is a lifecycle system that transforms raw recordings into stable training signals.

The most important design decision is not only the file format or the execution engine. It is deciding when data should remain dynamic and when it should become a training artifact.

That boundary is where most of the real architecture trade-off lives.
