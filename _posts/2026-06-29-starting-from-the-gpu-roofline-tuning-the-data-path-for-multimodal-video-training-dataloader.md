---
layout: post
title: "Starting from the GPU Roofline: Tuning the Data Path for Multimodal Video Training"
date: 2026-06-29
categories: [ml-infrastructure, data-pipeline, distributed-systems]
tags: [multimodal, video-training, ray-data, ray-train, data-pipeline, gpu-utilization, etl]
---

In a [previous post](https://zhenyu.github.io/2026/05/15/large-scale-multimodal-training-data-pipelines/), I wrote about design patterns for large-scale multimodal training data pipelines: metadata + blobs, distributed streaming DAGs, and pre-sharded training artifacts.

The main difference between these patterns is not the tool. It is **which work should remain dynamic, and which work should be materialized into a training-time artifact**.

This post is a concrete case study from that design space.

It is not a Ray Data tutorial, and it is not a checklist of parameters I tuned. The real question is more basic: when GPU utilization is low, should we scale the data path, change the data layout, or stop optimizing the dataloader entirely?

My conclusion is simple:

> The goal of data pipeline tuning is not to make the loader infinitely fast. It is to make the GPU stop waiting for data under the largest per-device batch or micro-batch allowed by the current training recipe.

The observations in this post come from a specific video training workload, with details abstracted where needed. The discussion focuses on a CPU decode + Ray Data streaming path. It does not cover GPU video decoding, model architecture changes, or training algorithm changes. The important part is the bottleneck reasoning process, not the absolute throughput number.

---

## 0. The one-line model

My mental model is:

```text
training throughput = min(GPU compute / memory roofline, data supply roofline)
```

This is not the strict FLOPs-vs-memory-bandwidth roofline model. I am using roofline here as an engineering mental model: training throughput is bounded by both the GPU-side compute/memory ceiling and the data-supply ceiling. The lower one determines actual throughput.

So before tuning the dataloader, do not touch the dataloader.

The first step is to remove loading from the equation. Use a small controlled dataset, a controlled read path, or even fake batches if needed. Ask the GPU-side question first:

- As batch size increases, does compute start to dominate?
- Does compute become the first bottleneck, or does memory hit the limit first?
- If memory hits the limit first, what is the largest per-device batch or micro-batch allowed by the training recipe?

Only after the GPU-side workload is fixed does `data_wait` become meaningful. At that point, the question is:

> Under the largest per-device batch or micro-batch allowed by this recipe, is the GPU worker still waiting for the next batch?

That was the tuning order in this case: **first establish the GPU roofline, then raise the data roofline**.

Ray Data was useful because it turned the data-supply side into an independently scalable distributed pipeline. ETL was useful because it moved repeated per-epoch work across the materialization boundary.

---

## 1. Find the GPU-side ceiling before tuning the loader

A lot of data loading work starts with low GPU utilization and immediately jumps to adding workers, increasing prefetch, or changing storage.

I think that order is risky.

Low GPU utilization can mean at least three different things:

- the batch is too small, so the GPU is not being exercised;
- the batch cannot be increased because memory hits the limit first;
- the data pipeline really is too slow, and the GPU is waiting for batches.

These have completely different fixes.

The first case needs a larger batch or heavier model-side work.  
The second needs more memory, model changes, activation/memory optimization, or a different batch strategy.  
Only the third is primarily a data pipeline problem.

So the first experiment I ran was a “remove the data variable” experiment. I used a small dataset, controlled the read path, and used fake batches where useful. The goal was not to measure final throughput. The goal was to establish the GPU-side load curve: as batch size increases, does forward/backward become the main component, and when does memory hit the wall?

The fake batch needs to preserve the real batch shape, dtype, device transfer path, and model input structure as much as possible. Otherwise it only measures pure model compute, not the full trainer step.

This baseline pins down the optimization target:

> Reduce GPU wait under the largest per-device batch allowed by the current training recipe.

Without this baseline, `data_wait` is hard to interpret. A small batch can amplify data wait even when the data pipeline is not fundamentally bad. A batch that is capped by GPU memory should not be “fixed” by adding more CPU decode capacity.

---

## 2. `data_wait` measures GPU-facing wait, not total data pipeline cost

The profile itself is not the main point of the post. It is only a tool for answering the roofline question: is the trainer computing, or is it waiting?

The `data_wait` I care about is **GPU-facing wait**: after one training step finishes, how long does the worker block before the next batch is actually handed to the trainer?

Ray Data may still be reading from S3, decoding, collating, transferring, and prefetching in the background. If that work is overlapped with GPU compute, then it is not GPU wait.

This distinction matters. I am not trying to prove that the data pipeline has no cost. I am asking a narrower question:

> Under the current maximum per-device batch or micro-batch, is the data side still making the GPU sit idle?

GPU utilization sampling is only supporting evidence. For short jobs, there may be too few samples. GPU sampling also sees only the GPU process. It does not see the CPU decode pool, the Ray object store, or operator backpressure.

For full supply-side observability, the pipeline needs cluster-level metrics such as Ray/Prometheus metrics. But for answering whether the GPU is blocked by data, worker-side `data_wait` is more direct.

There is one more caveat: `data_wait` answers a critical-path wait question, not the full end-to-end throughput story. Decode or collate cost may be hidden during steady-state steps by prefetch, while startup, tail latency, object store spilling, repartitioning, or shuffle can still stretch epoch wall-clock. Per-step profile and overall wall-clock need to be read together.

Also, CUDA kernels are submitted asynchronously. Without fixed synchronization points, some GPU work may be delayed into iterator boundaries, H2D copy, or the next step. So this profile is best used to determine whether GPU-facing wait is on the critical path, not to assign perfect absolute attribution to every substage. For that, a CUDA timeline or PyTorch profiler is needed.

---

## 3. Why Ray Data: moving the data roofline independently

The training sample in this pipeline looked roughly like this: multiple camera video frames, multiple forms of ground truth, and multiple task heads. The data lived in object storage. Downloading everything locally first was not a realistic answer. The video was stored in compressed form, so every epoch had to turn bitstreams back into pixels.

Ray Data was not useful because it magically made video decode faster. It was useful because it separated the data-supply side from the trainer-local loader:

```text
S3 / metadata + blobs
  -> distributed read / decode / transform
  -> streaming batches with backpressure
  -> GPU trainer
```

This corresponds to Pattern B in my previous post: the distributed streaming DAG.

Its design contract is not “automatically faster than a pre-sharded loader.” Its design contract is that **CPU-heavy data stages can scale separately from GPU-heavy training stages**.

Once that separation exists, read/decode operator concurrency becomes a meaningful control. If the GPU is waiting for data, increase CPU decode concurrency. If increasing it stops helping, then the bottleneck is probably not the number of parallel slots. It may be per-sample cost, data layout, or the GPU side already reaching its ceiling.

That is the tuning taste I care about:

> Identify which roofline is lower, then move the control that affects that roofline.

---

## 4. First data roofline: adding CPU helps, but it is not the root fix

The original version had a very common problem: high-resolution MP4s were decoded at runtime, while the model only needed a lower training input resolution.

The model side already had multiple task heads, but the training workers were still spending a large amount of time waiting for batches.

Increasing `read_concurrency` / decode concurrency was the right first move because it validated an important fact: Ray Data could move decode out of the GPU training process and raise the supply side by using an external CPU pool. As concurrency increased, wall-clock improved and GPU wait decreased.

But the experiment also showed the boundary. After a point, adding more CPU had diminishing returns.

This is where it is important not to keep tuning random parameters. If raw S3 reads are fast but the trainer is still waiting, the problem is probably not “bytes cannot be read.” The heavy work inside the ReadTask is video decode.

The first decision point was:

```text
adding CPU helps       -> the data supply roofline was indeed too low
adding CPU plateaus    -> per-sample decode cost has become the lower bound
```

So Ray Data CPU scaling is a safety valve, not the final answer. It tells you where the bottleneck is, and it can buy time. But if every epoch repeatedly decodes pixels that the model will never use, the right long-term answer is not to keep buying more CPU.

---

## 5. The real leverage: moving the materialization boundary

This goes back to the core question from the previous post:

> Which work should remain dynamic, and which work should be materialized into a training artifact?

Runtime decode of high-resolution video followed by resize is deterministic repeated work. If the model input resolution is already fixed, that work should not remain in the training critical path.

The biggest optimization was not changing the framework. It was ETL: re-encode the video into training resolution when building the dataset.

The decode cost is then paid once, not once per epoch, per worker, per GPU.

The benefit was order-of-magnitude. The same content encoded at training resolution is much easier to decode than “decode the full high-resolution bitstream, then downscale.” This benefit does not depend on Ray. Any loader that repeatedly decodes high-resolution video at runtime pays this tax.

This is also how I think about questions like “Ray Data vs WebDataset vs MDS vs TFRecord.” Tool choice should come after boundary choice.

First decide which processing steps should move into a training artifact. Then choose the format and execution engine that best serve that decision.

---

## 6. Bottleneck migration: once decode drops, collate appears

After ETL reduced the decode cost, GPU compute rose significantly. Work that had previously been hidden under decode started to show up.

The next bottleneck was collate. More specifically, the ground truth still existed as JSON strings inside the episode data, and runtime collate had to parse that JSON for every batch.

When decode was slow, JSON parsing looked unimportant. Once decode dropped, it became a large part of the critical path.

This is an easy mistake to make: an early profile may show a small collate percentage, but that does not mean collate is not worth optimizing. It may simply be diluted by a larger bottleneck. Roofline tuning often works this way: only after one bottleneck is fixed does the next one become eligible to appear.

The fix was the same kind of boundary movement: pre-decode structured ground truth into Arrow columns during ETL. At runtime, the pipeline only needed columnar reads and reshaping, not JSON parsing for every batch.

I prefer this over adding an in-memory LRU cache around JSON parsing. An LRU cache hides repeated cost inside each worker’s Python heap. It has unclear memory semantics and a hit-rate problem that now needs to be managed. Arrow columns turn the data into a training-time structure, reduce runtime JSON parsing and Python object construction, and fit more naturally into Ray Data’s columnar block processing.

---

## 7. Decoder optimization: useful, but only after reducing the work

Switching to a decoder such as Decord, which is better suited for video batch/random access, also helped. Its `get_batch(indices)` interface is useful for temporal window sampling.

This matters because a sample often needs not just one frame, but a sequence of `L` frames around a timestamp. Although compressed video random access is still constrained by keyframes and GOP structure, a batch/random-access decoder can avoid a lot of unnecessary frame-by-frame walking compared with a naive sequential path.

But I would still put this after ETL in the tuning story.

Decoder optimization is often a few-fold improvement. Moving high-resolution runtime decode out of the training path can be an order-of-magnitude improvement.

The tuning rule is:

```text
first reduce the work that must be done,
then optimize the implementation of the remaining work
```

If the system is still decoding the full high-resolution bitstream at runtime, changing the decoder helps, but it is optimizing an operation that should not have been repeated in the first place.

---

## 8. After fixing per-device batch, check whether the data side is still worth tuning

Now we return to the GPU roofline.

After ETL, materializing ground truth into Arrow columns, and switching the decoder, I checked GPU wait again under the largest per-device batch allowed by the current training recipe. Then I swept CPU decode concurrency again.

The result was clear: a small amount of decode concurrency was already enough to feed the GPU. Adding more CPU had little marginal benefit.

That is the point where I consider the data-side question closed. Not because `data_wait` became exactly zero, but because:

- per-device batch / micro-batch was fixed by memory or by the training recipe;
- GPU compute had become the dominant component;
- adding data-side CPU no longer significantly reduced GPU wait;
- the remaining wait looked more like iterator/prefetch boundaries, framework synchronization, or profiling synchronization points than decode work that could be solved by more parallelism.

At this point, continuing to tune the dataloader is no longer the highest-leverage work.

To improve overall throughput further, the next questions should move to larger GPU memory, a better per-device batch / gradient accumulation strategy, more GPU workers, or more detailed tracing inside the training loop and DDP synchronization.

---

## 9. Decision rules from this tuning pass

The reusable part of this work is not a specific percentage. It is the decision order.

| Observation | Interpretation | Next step |
|---|---|---|
| Per-device batch can still increase under small/fake data | GPU side is not fixed yet | First find the compute/memory roofline |
| Increasing per-device batch hits OOM | Memory is the GPU-side constraint | Lock in the largest recipe-allowed per-device batch, then inspect data wait |
| GPU waits for batches under fixed per-device batch | Data supply roofline is too low | Increase `read_concurrency` / decode concurrency |
| Adding CPU helps but quickly plateaus | Per-sample cost is too high | Change layout / ETL instead of adding more CPU |
| Collate rises after decode drops | Bottleneck migration | Move repeated parse / transform work into ETL |
| `data_wait` is low and CPU sweep no longer helps | Data side is no longer the main bottleneck | Stop tuning the loader; move to GPU / DDP / memory work |

The same decision order explains the trade-off between eager ETL and runtime flexibility:

| Path | Advantages | Cost | Best fit |
|---|---|---|---|
| eager ETL | Low per-sample cost; a small CPU pool can feed GPUs | Changes require rematerialization | Production training; stable training view |
| runtime flexible | Easy to change sampling, resolution, or view logic | Higher CPU and object-store pressure | Research exploration; rapidly evolving data view |

This is not a binary choice. A mature system often needs both: keep more dynamicity early, then move repeated work forward once the view stabilizes.

---

## 10. The order of controls in this pipeline

If I only list Ray Data API parameters, this becomes a checklist. My ordering is based on the roofline reasoning instead:

| Control | What it changes | When to use it |
|---|---|---|
| per-device batch / micro-batch | GPU-side load | Set this first to find the compute/memory roofline |
| `read_concurrency` / decode concurrency | CPU decode parallelism | Use after fixing per-device batch, when GPU wait is high |
| ETL to training resolution | Per-sample decode cost | Use when CPU scaling plateaus and decode cost is the lower bound |
| ETL ground truth into Arrow columns | Runtime parse cost | Use when collate appears after decode drops |
| Decoder replacement | Implementation cost of necessary decode | Use for random access / L-frame windows or slow decode implementation |
| `prefetch_batches` | Overlap between data work and GPU compute | Use when worker wait mainly comes from boundary gaps |
| block/file-level shuffle + local shuffle buffer | Shuffle quality under streaming constraints | Use to avoid full materialization and keep bounded buffers |

Shuffle is especially easy to misunderstand.

Ray Data streaming execution does not mean “materialize the whole dataset, then start training.” For large datasets, a shuffle strategy that requires full materialization can blow up object store pressure. A more practical approach is block/file-level reordering plus a local reservoir buffer during iteration.

This is not equivalent to a strict global random permutation, but for very large streaming training workloads, it is often a better trade-off between randomness, throughput, and resource cost.

---

## 11. Connecting back to the previous post

In the previous post, I argued that the core question in a multimodal training data pipeline is the materialization boundary:

```text
Which stages remain dynamic, and which stages become stable training artifacts?
```

This case study made that framing feel even more useful.

Ray Data is a good fit for a dynamic distributed streaming DAG. It can move CPU-heavy read/decode/transform work out of the GPU trainer and scale it independently. But when a transform becomes stable and repeats every epoch, it should be moved forward.

So the conclusion is not “Ray Data wins” or “ETL wins.” A better summary is:

> Ray Data gave me a control knob for moving the data-supply roofline. The roofline profile told me when to keep scaling and when to move the materialization boundary.

That is the interesting part of this kind of tuning. Good system tuning is not about turning every knob. It is about knowing which knob corresponds to the current bottleneck.

The best final state is not that the dataloader becomes infinitely fast. It is that I no longer need to keep turning the data-side CPU knob. The data path has moved back to where it should be. The remaining problems belong to the training side itself: GPU memory, compute, batch strategy, and distributed training synchronization.
