# POSIX Concurrency Systems (C)

This repository explores POSIX-based concurrency through two MapReduce-style programs on a single host:
1) Parallel Sorting (MapReduce-style)
2) Max-Value Aggregation (shared-state reduction)

It includes thread-based (pthreads) and process-based (fork + IPC/shared memory) implementations and a short empirical analysis of correctness, scalability, runtime, and memory trade-offs.

> **Experimental note:** Results below were measured on a single local machine and are intended for comparative/educational analysis rather than universal benchmarking. </br>
> **Platform Notes:** This project targets POSIX-compliant systems and was developed and tested on Ubuntu Linux. Native Windows is not supported; WSL can be used instead.


---

## Part 1 - Parallel Sorting (MapReduce Style)

### Overview
Two variants were implemented:
- **Multithreading (pthreads):** Map = sort chunks in parallel, Reduce = merge sorted subarrays
- **Multiprocessing (fork + pipes):** Map = sort per-process chunks, Reduce = merge results in parent

### Results - Threads
| Workers | MapTime (ms) | ReduceTime (ms) | Total (ms) | MaxRSS (KB) |
|---:|---:|---:|---:|---:|
| 1 | 22.131 | 0.721 | 22.852 | 2676 |
| 2 | 10.045 | 2.073 | 12.118 | 2680 |
| 4 | 5.188 | 3.173 | 8.362 | 2680 |
| 8 | 4.640 | 4.095 | 8.735 | 4208 |

**Interpretation**
- Speedup improves up to moderate worker counts (e.g., 4 workers), then plateaus due to merge and scheduling overhead.
- Memory stays nearly flat until higher worker counts, where per-thread stacks increase RSS.

### Results - Processes
| Workers | MapTime (ms) | ReduceTime (ms) | Total (ms) | MaxRSS_self (KB) | MaxRSS_children (KB) |
|---:|---:|---:|---:|---:|---:|
| 1 | 24.462 | 0.697 | 25.159 | 2680 | 1660 |
| 2 | 10.452 | 2.158 | 12.610 | 2680 | 1124 |
| 4 | 6.103 | 3.371 | 9.474 | 2680 | 932 |
| 8 | 5.618 | 3.940 | 9.558 | 2680 | 800 |

### Threads vs Processes (Summary)
- Both models scale similarly up to moderate worker counts.
- Processes are slightly slower due to `fork()` and IPC overhead, while threads benefit from shared memory.
- Process isolation provides stronger safety boundaries (independent address spaces).

---

## Part 2 - Max-Value Aggregation (MapReduce Style)

### Threads: Naive vs Mutex vs Atomic (CAS)
Each thread computes a local maximum and updates a shared global value.
Three modes were evaluated:
- **naive:** no synchronization (race-prone)
- **mutex:** mutual exclusion
- **atomic:** CAS-based update

| Workers | Mode  | MapTime (ms) | Total (ms) | MaxRSS (KB) |
|---:|---|---:|---:|---:|
| 1 | mutex  | 0.344 | 0.344 | 2428 |
| 2 | mutex  | 0.388 | 0.388 | 2428 |
| 4 | mutex  | 0.424 | 0.424 | 2428 |
| 8 | mutex  | 0.717 | 0.717 | 2428 |
| 8 | atomic | 0.690 | 0.690 | 2428 |
| 8 | naive  | 0.763 | 0.763 | 2428 |

**Interpretation**
- Mutex/atomic modes are correct by construction; naive is race-prone (may appear correct depending on schedule/seed).
- For fine-grained workloads, synchronization overhead dominates and adding workers does not improve runtime.
- Atomic updates can be competitive for single-variable shared-state updates.

### Processes: Shared Memory + Semaphore
Each child process computes a local maximum and updates a shared-memory integer protected by a POSIX semaphore.

| Workers | Total (ms) | MaxRSS Self (KB) | MaxRSS Children (KB) |
|---:|---:|---:|---:|
| 1 | 0.457 | 2428 | 820 |
| 2 | 0.457 | 2428 | 820 |
| 4 | 0.761 | 2428 | 884 |
| 8 | 1.040 | 2428 | 816 |

**Interpretation**
- Results are correct across worker counts due to semaphore-protected updates.
- Runtime increases slightly with more processes due to `fork()` and semaphore overhead.
- Children RSS stays relatively low, consistent with copy-on-write behavior.

---

## Build & Run

The programs can be compiled using `gcc` on a POSIX-compatible system.

```bash
# Parallel sorting (threads)
gcc -O2 -pthread parallel_sort_pthreads.c -o parallel_sort_pthreads

# Parallel sorting (processes)
gcc -O2 parallel_sort_process.c -o parallel_sort_process

# Max-value aggregation (threads)
gcc -O2 -pthread max_aggregation_pthreads.c -o max_aggregation_pthreads

# Max-value aggregation (processes)
gcc -O2 max_aggregation_process.c -o max_aggregation_process

