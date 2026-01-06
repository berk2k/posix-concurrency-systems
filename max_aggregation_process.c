// Part 2B — Max-Value Aggregation (Processes): single‑int shared buffer + POSIX semaphore
//
// Model
// -----
// • Parent allocates the input array (malloc). Children read it via copy‑on‑write pages.
// • A *single* shared-memory integer holds the global maximum (MAP_SHARED|ANONYMOUS mmap).
// • Synchronization uses a POSIX *named semaphore* (sem_open). This keeps the
//   data buffer truly one integer (the semaphore is a kernel object, not stored
//   inside our single‑int data region).
// • Each child computes a local max over its chunk and, under the semaphore,
//   updates the shared integer iff local_max > global.
// • Parent waits all children, then reads the final value (reduce). Optional
//   verification compares with a sequential maximum.
//
// Build
//   gcc -O2 -std=c11 -Wall -Wextra -pthread -o part2_process_max part2_process_max.c -lrt
//   (On modern glibc, -lrt is often unnecessary; kept for portability.)
//
// Run
//   ./part2_process_max -n 131072 -w 1 -v
//   ./part2_process_max -n 131072 -w 2 -v
//   ./part2_process_max -n 131072 -w 4 -v
//   ./part2_process_max -n 131072 -w 8 -v
//
// Notes
//   • If sem_open fails on your platform, a common fallback is an unnamed
//     semaphore placed in a separate shared mapping (sem_init(pshared=1)).
//     We keep the data buffer as a lone int to match the assignment’s constraint.

#define _POSIX_C_SOURCE 200809L
#include <errno.h>
#include <fcntl.h>
#include <limits.h>
#include <semaphore.h>
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

static inline long long now_ns(void){ 
	struct timespec ts; 
	clock_gettime(CLOCK_MONOTONIC,&ts); 
	return (long long)ts.tv_sec*1000000000LL+ts.tv_nsec; 
}
static inline double ns_to_ms(long long ns){ 
	return (double)ns/1.0e6; 
}
static long get_max_rss_self(void){ 
	struct rusage ru; 
	if(getrusage(RUSAGE_SELF,&ru)!=0) return -1; 
	return ru.ru_maxrss; 
}
static long get_max_rss_children(void){ 
	struct rusage ru; 
	if(getrusage(RUSAGE_CHILDREN,&ru)!=0) return -1; 
	return ru.ru_maxrss; 
}

static void fill_random(int *a, size_t n, unsigned seed){ 
	uint32_t x=seed?seed:2463534242u; 
	for(size_t i=0;i<n;++i){ 
		x^=x<<13; x^=x>>17; 
		x^=x<<5; a[i]=(int)(x & 0x7fffffff);
	} 
}
static int max_sequential(const int *a, size_t n){ 
	int m=a[0]; 
	for(size_t i=1;i<n;++i) if(a[i]>m) m=a[i]; 
	return m; 
}

static void usage(const char *p){
    fprintf(stderr,
        "Usage: %s [-n length] [-w workers] [-s seed] [-v]\n"
        "  -n    total array length (default 131072)\n"
        "  -w    number of workers (1,2,4,8…) (default 4)\n"
        "  -s    RNG seed (default 12345)\n"
        "  -v    verify result against sequential max\n", p);
}

int main(int argc, char **argv){
    size_t n = 131072, workers = 4; unsigned seed = 12345; int verify = 0;
    int auto_mode = 0;  
    int opt;
    while ((opt = getopt(argc, argv, "n:w:s:vha")) != -1) {
        switch (opt) {
            case 'n': n = (size_t)strtoull(optarg, NULL, 10); break;
            case 'w': workers = (size_t)strtoull(optarg, NULL, 10); break;
            case 's': seed = (unsigned)strtoul(optarg, NULL, 10); break;
            case 'v': verify = 1; break;
            case 'a': auto_mode = 1; break;  
            case 'h': default: usage(argv[0]); return 2;
        }
    }

    if (auto_mode) {
        int list[] = {1, 2, 4, 8};
        for (int i = 0; i < 4; i++) {
            char cmd[128];
            snprintf(cmd, sizeof(cmd),
                     "./part2_process_max -n %zu -w %d%s",
                     n, list[i], verify ? " -v" : "");
            printf("\n>>> Running with %d workers...\n", list[i]);
            (void)system(cmd);
        }
        return 0;
    }

    if (workers == 0) workers = 1;
    if (workers > n) workers = n;

    // Input array
    int *arr = (int*)malloc(sizeof(int) * n);
    if (!arr) { perror("malloc"); return 1; }
    fill_random(arr, n, seed);

    // Shared single-int buffer for global max
    int *gmax = mmap(NULL, sizeof(int),
                     PROT_READ | PROT_WRITE,
                     MAP_SHARED | MAP_ANONYMOUS, -1, 0);
    if (gmax == MAP_FAILED) { perror("mmap gmax"); return 1; }
    *gmax = INT_MIN;

    // Semaphore
    char sem_name[64];
    snprintf(sem_name, sizeof(sem_name), "/maxsem_%ld", (long)getpid());
    sem_t *sem = sem_open(sem_name, O_CREAT | O_EXCL, 0600, 1);
    if (sem == SEM_FAILED) {
        perror("sem_open");
        fprintf(stderr, "Hint: try unnamed semaphore if this fails.\n");
        return 1;
    }
    sem_unlink(sem_name);

    size_t base = n / workers, rem = n % workers;
    long long t0 = now_ns();

    size_t started = 0;
    for (size_t i = 0; i < workers; ++i) {
        pid_t pid = fork();
        if (pid < 0) { perror("fork"); return 1; }
        if (pid == 0) {
            size_t start = i * base + (i < rem ? i : rem);
            size_t len = base + (i < rem ? 1 : 0);
            if (len > 0) {
                int local_max = arr[start];
                for (size_t k = 1; k < len; ++k) {
                    int v = arr[start + k];
                    if (v > local_max) local_max = v;
                }
                if (sem_wait(sem) == -1) _exit(111);
                if (local_max > *gmax) *gmax = local_max;
                if (sem_post(sem) == -1) _exit(112);
            }
            _exit(0);
        } else started++;
    }

    size_t reaped = 0; int status = 0;
    while (reaped < started) {
        pid_t w = wait(&status);
        if (w > 0) reaped++;
        else if (errno == EINTR) continue;
        else break;
    }

    long long t1 = now_ns();
    int global_max = *gmax;
    long long t2 = now_ns();

    if (verify) {
        int truth = max_sequential(arr, n);
        if (truth != global_max) {
            fprintf(stderr, "ERROR: wrong global max! got=%d expected=%d\n",
                    global_max, truth);
            return 1;
        }
    }

    long rss_self = get_max_rss_self();
    long rss_children = get_max_rss_children();
    printf("ArrayLength=%zu Workers=%zu Seed=%u\n", n, workers, seed);
    printf("MapTime_ms=%.3f ReduceTime_ms=%.3f Total_ms=%.3f\n",
           ns_to_ms(t1 - t0), ns_to_ms(t2 - t1), ns_to_ms(t2 - t0));
#if defined(__linux__)
    printf("MaxRSS_self=%ld KB MaxRSS_children=%ld KB\n",
           rss_self, rss_children);
#else
    printf("MaxRSS_self=%ld bytes MaxRSS_children=%ld bytes\n",
           rss_self, rss_children);
#endif
    printf("GlobalMax=%d\n", global_max);

    sem_close(sem);
    munmap(gmax, sizeof(int));
    free(arr);
    return 0;
}

