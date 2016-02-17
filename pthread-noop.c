#include <pthread.h>

int pthread_mutex_lock(pthread_mutex_t *m){ return 0; }
int pthread_mutex_unlock(pthread_mutex_t *m) { return 0; }
int pthread_rwlock_rdlock(pthread_rwlock_t *rw) { return 0; }
int pthread_rwlock_wrlock(pthread_rwlock_t *rw) { return 0; }
int pthread_rwlock_unlock(pthread_rwlock_t *rw) { return 0; }
int pthread_cond_signal(pthread_cond_t *c) { return 0; }
int pthread_cond_broadcast(pthread_cond_t *c) { return 0; }
int pthread_cond_wait(pthread_cond_t *c, pthread_mutex_t *m) { return 0; }
