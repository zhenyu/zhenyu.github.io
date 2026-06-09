---
layout: post
title: "How to Get Tensor Core Metrics from a SageMaker Training Job"
date: 2026-06-05
categories: [ml-infrastructure, gpu, observability]
tags: [sagemaker, gpu-profiling, dcgm, nvml, gpm, h100, hopper, tensor-cores]
---

W&B will happily tell you your GPU is at "100% utilization." It will not tell you whether the workload actually hit the tensor-instruction path, whether the SMs were meaningfully occupied, or whether the memory subsystem was the real bottleneck.

Those are very different facts. A default dashboard usually shows only one of them.

I went looking for the second fact — Tensor Core activity, SM occupancy, DRAM bandwidth — inside a normal SageMaker training job. It turned into a satisfying little rabbit hole about Linux capabilities, two different ways NVIDIA exposes GPU data, and one narrow door that happens to be open on Hopper and newer GPUs.

This post is the map I wish I'd had.

## The gap

`nvidia-smi` and W&B's built-in system monitor both answer "is a kernel running?" They show you `gpu.0.gpu` utilization, memory, temperature, power, and clocks. None of that tells you how full the silicon actually is.

"Utilization 100%" can mean your SMs are saturated doing real work. It can also mean one small kernel is keeping the GPU nominally busy while the expensive units sit mostly idle.

The metrics that answer "how full" are things like:

- **SM active / SM occupancy** — are the streaming multiprocessors actually working, and how packed are they?
- **Tensor pipe active** — are you using the Tensor Cores at all?
- **DRAM active** — are you memory-bandwidth bound?

These are exactly the kind of signals tools like DCGM and Systalyze's `utilyze` surface. So I tried to wire DCGM into a SageMaker job. It failed in a way that, once I understood it, explained everything.

## Two kinds of GPU data

Here's the thing that makes this confusing until it suddenly isn't. "Profile my training" splits into two categories with different data paths and different privilege requirements:

| Question | Where the data comes from | Extra privilege in a restricted container? |
|---|---|---|
| When did each kernel run, how long, what gap between them | CUPTI **Activity** API, your own process's events | **No** |
| How full were the SMs, Tensor Cores, and memory bus | hardware **performance counters** | **Yes** — typically admin counter access / `CAP_SYS_ADMIN` |

This is why PyTorch Profiler's default timeline works almost anywhere. It is the Activity API recording your kernels' timestamps.

The moment you ask for hardware-counter data — SM activity, Tensor Core activity, memory-system activity — you are touching a global, cross-process hardware resource. NVIDIA gates that behind the driver's performance-counter permission model. In a restricted Linux container, that effectively lands on `CAP_SYS_ADMIN`.

The reasoning is sound. Performance counters are a side-channel risk in multi-tenant environments, and `CAP_SYS_ADMIN` is a broad, dangerous capability.

So the question "can I get Tensor Core metrics in SageMaker?" reduces to a very concrete one: does my SageMaker training container have the capability needed by the normal profiling path?

## The missing piece: SageMaker training containers don't have `CAP_SYS_ADMIN`

I confirmed this four independent ways, because it kept feeling like surely I was just holding it wrong.

1. **The error.** DCGM's `dcgmi dmon` for the profiling fields returns `Error setting watches. Result: -29: ... requires the host engine to be running as root.` The message says "root," but the real boundary is the capability / admin counter-access path.

2. **A controlled local A/B.** Same image, same container. With `docker run --cap-add SYS_ADMIN`, the SM/Tensor fields stream. Without it, the identical `-29` appears. So it is the capability, not the user id. Container "root" is not the same thing as `CAP_SYS_ADMIN`.

3. **The API has no knob.** AWS's `CreateTrainingJob` API has no field for Linux capabilities or privileged mode. You can set the image, environment, resource config, VPC, entrypoint, debugger options, and so on. There is nowhere to ask SageMaker to add a container capability.

4. **I measured it.** I ran a probe that printed `/proc/self/status`. The container's effective capabilities decode to Docker's default set: `CHOWN`, `DAC_OVERRIDE`, `FOWNER`, `FSETID`, `KILL`, `SETGID`, `SETUID`, `NET_BIND_SERVICE`, `SYS_CHROOT`, and `AUDIT_WRITE`. `CAP_SYS_ADMIN` — bit 21 — is not in it. Same mask on A10G and H100.

A useful detail: in the environments I tested, this behaved like a platform-level policy, not a GPU-specific property. Even on the large exclusive instances where one job owns the physical box, the capability is still withheld. Since the capability mask was identical across the instance types I tested, the bottleneck is the way SageMaker starts the container, not the particular GPU.

So: in a stock SageMaker training job, DCGM and `utilyze`-style profiling counters are effectively unavailable. They require a capability that SageMaker does not expose through the training-job API.

One aside: AWS's own GPU health-check sample can run `dcgmi diag` successfully on SageMaker. That briefly fooled me. But `diag` is a self-test path; it is not evidence that DCGM profiling counters are available.

## The workaround: NVML GPM

Here's the door that's open.

NVIDIA's NVML library has a feature called **GPM** — GPU Performance Monitoring: `nvmlGpmQueryDeviceSupport`, `nvmlGpmSampleGet`, and `nvmlGpmMetricsGet`.

GPM is not a full replacement for DCGM profiling. It is the path that matters for this particular problem: it exposes an overlapping class of GPU-efficiency signals — SM utilization, SM occupancy, tensor activity, DRAM bandwidth, FP16/FP32/FP64 utilization, PCIe throughput, NVLink throughput — through the NVML driver interface rather than the DCGM profiling-counter path.

And critically, in a SageMaker H100 training container with the default capability set, that NVML GPM path works without `CAP_SYS_ADMIN`.

There is one catch: **GPM is implemented on Hopper and newer GPUs**. On anything older — including A100 and A10G — `nvmlGpmQueryDeviceSupport` returns unsupported.

I verified both halves on actual SageMaker hardware with a tiny probe: no DCGM, no extra capability, no training loop, just NVML.

| Instance | GPU | `isSupportedDevice` | Sample without `CAP_SYS_ADMIN`? |
|---|---|---|---|
| `ml.g5.xlarge` | A10G, Ampere | **False** | unsupported |
| `ml.p5.48xlarge` | H100, Hopper | **True** | **Yes** — sampled `sm_util` / `sm_occupancy` with the container's default caps |

That second row is the punchline. The H100 container had the exact same capability set as the A10G one — no `CAP_SYS_ADMIN` — and GPM sampled fine anyway.

The point is not H100 by itself. The point is that Hopper and newer GPUs expose this NVML GPM path, while pre-Hopper GPUs do not.

So the decision tree for a SageMaker training job becomes:

```text
Want SM / Tensor Core / DRAM utilization in a SageMaker training job?
├─ DCGM / utilyze  → need CAP_SYS_ADMIN → ✗ (not granted, can't add)
└─ NVML GPM        → accessible through the non-privileged NVML path
                     ├─ pre-Hopper (T4/A10G/A100) → unsupported → ✗
                     └─ Hopper+ (H100/H200/B200)  → ✓  ← the one open door
```

If your training runs on Hopper or newer hardware — and a lot of large pretraining does — you can have Tensor Core and SM-efficiency metrics in a stock SageMaker training job. Everywhere else on managed SageMaker training, you are mostly limited to the device-level metrics that W&B and `nvidia-smi` already give you.

## Putting it together

The collection itself is refreshingly boring. It is pure NVML through `pynvml`: no daemon, no extra binary, no root.

You query whether the device supports GPM, take two samples a moment apart, and ask GPM for the metrics computed across that interval: SM utilization, SM occupancy, tensor activity, memory bandwidth utilization, and the rest. That's the whole mechanism.

To make it useful for real training, run it out of band: a background thread sampling on an interval and shipping the numbers to wherever your training metrics already live. For me, that meant attaching to the same W&B run as the trainer, so the GPU-efficiency curves sit next to the loss curves.

Two practical lessons from wiring that up:

- **Start it after `wandb.init()`**, inside the training process, so you can attach to the live run. A child process can't share that handle; a same-process background thread can.
- **Fail-isolate it.** A metrics collector must never slow down or crash the actual training. Sampling errors get logged and dropped, never raised.

## Takeaways

- "Profile my GPU" is two questions with two privilege levels. **Timeline = CUPTI Activity = no extra privilege. Efficiency/utilization counters = hardware-counter path = admin counter access / `CAP_SYS_ADMIN` in restricted containers.** Know which one you're asking for.
- **Managed SageMaker training does not grant `CAP_SYS_ADMIN`, and there is no training-job API field to add it.** So the DCGM / CUPTI-counter / `utilyze` profiling-counter family is blocked in a stock SageMaker training job.
- **NVML GPM is the exception**. It exposes the GPU-efficiency metrics I needed through a different, non-privileged NVML path, but only on Hopper and newer GPUs. On Hopper+, you can get Tensor Core and SM-efficiency metrics in a stock training job with nothing more than a pip dependency.
- The constraint is fundamentally about who controls the container's capabilities. On infrastructure you control, you can add `CAP_SYS_ADMIN` and the normal DCGM / `utilyze` profiling surface opens up. On a managed platform, you ride whatever non-privileged door the vendor and driver stack leave open. On SageMaker plus NVIDIA Hopper, GPM is that door.
