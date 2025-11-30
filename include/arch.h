#ifndef ARCH_H
#define ARCH_H

/* Architecture-specific CPU pause/yield */
#if defined(__x86_64__) || defined(__i386__)
    #include <immintrin.h>
    #define cpu_relax() _mm_pause()
#elif defined(__aarch64__) || defined(__arm__)
    #define cpu_relax() __asm__ __volatile__("yield" ::: "memory")
#else
    #define cpu_relax() do {} while (0)
#endif

#endif /* ARCH_H */
