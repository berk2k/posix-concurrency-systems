
#define _POSIX_C_SOURCE 200809L
#include <errno.h>
#include <fcntl.h>
#include <signal.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/mman.h>
#include <sys/resource.h>
#include <sys/types.h>
#include <sys/wait.h>
#include <time.h>
#include <unistd.h>

// ----------------------------- Timing helpers ------------------------------
static inline long long now_ns(void) {
    struct timespec ts; clock_gettime(CLOCK_MONOTONIC, &ts);
    return (long long)ts.tv_sec * 1000000000LL + ts.tv_nsec;
}
static inline double ns_to_ms(long long ns) { return (double)ns / 1.0e6; }

// --------------------------- Memory usage helper ---------------------------
static long get_max_rss_self(void) {
    struct rusage ru; if (getrusage(RUSAGE_SELF, &ru) != 0) return -1; return ru.ru_maxrss;
}
static long get_max_rss_children(void) {
    struct rusage ru; if (getrusage(RUSAGE_CHILDREN, &ru) != 0) return -1; return ru.ru_maxrss;
}

// ------------------------------ Data model ---------------------------------
typedef struct { size_t start; size_t len; } Chunk;

// ------------------------------ Utilities ----------------------------------
static void fill_random(int *a, size_t n, unsigned seed) {
    // xorshift32 — deterministic, portable
    uint32_t x = seed ? seed : 2463534242u;
    for (size_t i = 0; i < n; ++i) { x ^= x<<13; x ^= x>>17; x ^= x<<5; a[i] = (int)(x & 0x7fffffff); }
}
static int is_sorted(const int *a, size_t n) { for (size_t i=1;i<n;++i) if (a[i-1]>a[i]) return 0; return 1; }
static int cmp_int(const void *a, const void *b){int ia=*(const int*)a, ib=*(const int*)b; return (ia>ib)-(ia<ib);} 

// Robust read/write for full struct transfers over a pipe
static ssize_t write_full(int fd, const void *buf, size_t n){
    const char *p = (const char*)buf; size_t off=0; while(off<n){ ssize_t k=write(fd,p+off,n-off); if(k<0){ if(errno==EINTR) continue; return -1;} off+= (size_t)k;} return (ssize_t)off; }
static ssize_t read_full (int fd, void *buf, size_t n){ char *p=(char*)buf; size_t off=0; while(off<n){ ssize_t k=read(fd,p+off,n-off); if(k==0) break; if(k<0){ if(errno==EINTR) continue; return -1;} off+=(size_t)k;} return (ssize_t)off; }

// ------------------------------ K-way merge --------------------------------
typedef struct { int value; size_t run; size_t idx; } HeapNode;
typedef struct { HeapNode *a; size_t n; } MinHeap;

static void heap_swap(HeapNode *x, HeapNode *y){HeapNode t=*x; *x=*y; *y=t;}

static void heap_down(MinHeap *h, size_t i){ for(;;){ size_t l=2*i+1,r=l+1,m=i; if(l<h->n && h->a[l].value<h->a[m].value) m=l; if(r<h->n && h->a[r].value<h->a[m].value) m=r; if(m==i) break; heap_swap(&h->a[m],&h->a[i]); i=m; }}

static void kway_merge(const int *arr, int *out, const Chunk *chunks, size_t k){
    HeapNode *nodes = (HeapNode*)malloc(sizeof(HeapNode)*k); MinHeap h={.a=nodes,.n=0};
	
    for(size_t i=0;i<k;++i){ if(chunks[i].len==0) continue; h.a[h.n++]=(HeapNode)
	{ 
		.value=arr[chunks[i].start], .run=i, .idx=0}; 
	}
    
	for(ssize_t i=(ssize_t)h.n/2-1;i>=0;--i) heap_down(&h,(size_t)i);
    size_t out_i=0; while(h.n>0){ HeapNode root=h.a[0]; out[out_i++]=root.value; size_t run=root.run, idx=root.idx+1; if(idx<chunks[run].len){ h.a[0]=(HeapNode){ .value=arr[chunks[run].start+idx], .run=run, .idx=idx}; } else { h.a[0]=h.a[h.n-1]; h.n--; } if(h.n>0) heap_down(&h,0);} free(nodes);
}

// ------------------------------ Main program -------------------------------
static void usage(const char *p){
    fprintf(stderr,
        "Usage: %s [-n length] [-w workers] [-s seed] [-v]\n"
        "  -n    total array length (default 131072)\n"
        "  -w    number of workers (1,2,4,8…) (default 4)\n"
        "  -s    RNG seed (default 12345)\n"
        "  -v    verify final array is sorted\n", p);
}

int main(int argc, char **argv) {
    size_t n = 131072, workers = 4;
    unsigned seed = 12345;
    int verify = 0;
    int auto_mode = 0; 

    int opt;
    while ((opt = getopt(argc, argv, "n:w:s:hva")) != -1) {
        switch (opt) {
            case 'n': n = (size_t)strtoull(optarg, NULL, 10); break;
            case 'w': workers = (size_t)strtoull(optarg, NULL, 10); break;
            case 's': seed = (unsigned)strtoul(optarg, NULL, 10); break;
            case 'v': verify = 1; break;
            case 'a': auto_mode = 1; break;
            case 'h': default: usage(argv[0]); return 2;
        }
    }

    // Auto test mode (1, 2, 4, 8 workers)
    if (auto_mode) {
        int list[] = {1, 2, 4, 8};
        for (int i = 0; i < 4; i++) {
            char cmd[128];
            snprintf(cmd, sizeof(cmd),
                     "./part1_process_sort -n %zu -w %d%s",
                     n, list[i], verify ? " -v" : "");
            printf("\n>>> Running with %d workers...\n", list[i]);
            (void)system(cmd); 
        }
        return 0;
    }

    if (workers == 0) workers = 1;
    if (workers > n) workers = n;


    size_t bytes = n * sizeof(int);
    int *arr = mmap(NULL, bytes, PROT_READ | PROT_WRITE,
                    MAP_SHARED | MAP_ANONYMOUS, -1, 0);
    int *out = mmap(NULL, bytes, PROT_READ | PROT_WRITE,
                    MAP_SHARED | MAP_ANONYMOUS, -1, 0);
    if (arr == MAP_FAILED || out == MAP_FAILED) {
        perror("mmap");
        return 1;
    }

    fill_random(arr, n, seed);

    Chunk *chunks = (Chunk *)calloc(workers, sizeof(Chunk));
    if (!chunks) {
        perror("calloc");
        return 1;
    }

    size_t base = n / workers, rem = n % workers;
    for (size_t i = 0; i < workers; ++i) {
        size_t start = i * base + (i < rem ? i : rem);
        size_t len = base + (i < rem ? 1 : 0);
        chunks[i] = (Chunk){.start = start, .len = len};
    }

    int pipefd[2];
    if (pipe(pipefd) != 0) {
        perror("pipe");
        return 1;
    }

    long long t0 = now_ns();

    size_t started = 0;
    for (size_t i = 0; i < workers; ++i) {
        pid_t pid = fork();
        if (pid < 0) {
            perror("fork");
            return 1;
        }
        if (pid == 0) {
            // child
            close(pipefd[0]);
            Chunk me = chunks[i];
            if (me.len > 1)
                qsort(arr + me.start, me.len, sizeof(int), cmp_int);
            if (write_full(pipefd[1], &me, sizeof(me)) != (ssize_t)sizeof(me))
                _exit(111);
            _exit(0);
        } else {
            started++;
        }
    }

    close(pipefd[1]);
    Chunk *got = (Chunk *)calloc(workers, sizeof(Chunk));
    if (!got) {
        perror("calloc");
        return 1;
    }
    for (size_t i = 0; i < workers; ++i) {
        if (read_full(pipefd[0], &got[i], sizeof(Chunk)) != (ssize_t)sizeof(Chunk)) {
            fprintf(stderr, "ERROR: short read from pipe\n");
            return 1;
        }
    }
    close(pipefd[0]);

    size_t reaped = 0;
    int status = 0;
    while (reaped < started) {
        pid_t w = wait(&status);
        if (w > 0)
            reaped++;
        else if (errno == EINTR)
            continue;
        else
            break;
    }

    long long t1 = now_ns();
    kway_merge(arr, out, got, workers);
    long long t2 = now_ns();

    if (verify && !is_sorted(out, n)) {
        fprintf(stderr, "ERROR: output is not sorted!\n");
        return 1;
    }

    long rss_self = get_max_rss_self();
    long rss_children = get_max_rss_children();
    printf("ArrayLength=%zu Workers=%zu Seed=%u\n", n, workers, seed);
    printf("MapTime_ms=%.3f ReduceTime_ms=%.3f Total_ms=%.3f\n",
           ns_to_ms(t1 - t0), ns_to_ms(t2 - t1), ns_to_ms(t2 - t0));
#if defined(__linux__)
    printf("MaxRSS_self=%ld KB MaxRSS_children=%ld KB\n",
           rss_self, rss_children);
#endif

    munmap(arr, bytes);
    munmap(out, bytes);
    free(chunks);
    free(got);
    return 0;
}

