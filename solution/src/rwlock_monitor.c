#include "sync_utils.h"
#include <errno.h>

int rwm_init(rwlock_monitor_t *rw) {
  if (!rw) {
    errno = EINVAL;
    return -1;
  }
  rw->AR = 0;
  rw->WR = 0;
  rw->AW = 0;
  rw->WW = 0;
  rw->writer_batch_count = 0;
  rw->phase = RWM_PHASE_WRITER;
  rw->next_reader_ticket = 0;
  rw->reader_batch_end = 0;
  rw->reader_batch_remaining = 0;

  int err = pthread_mutex_init(&rw->m, NULL);
  if (err) {
    errno = err;
    return -1;
  }
  err = pthread_cond_init(&rw->can_read, NULL);
  if (err) {
    pthread_mutex_destroy(&rw->m);
    errno = err;
    return -1;
  }
  err = pthread_cond_init(&rw->can_write, NULL);
  if (err) {
    pthread_cond_destroy(&rw->can_read);
    pthread_mutex_destroy(&rw->m);
    errno = err;
    return -1;
  }
  return 0;
}

void rwm_destroy(rwlock_monitor_t *rw) {
  pthread_cond_destroy(&rw->can_read);
  pthread_cond_destroy(&rw->can_write);
  pthread_mutex_destroy(&rw->m);
}

void rwm_rlock(rwlock_monitor_t *rw) {
  pthread_mutex_lock(&rw->m);
  uint64_t ticket = rw->next_reader_ticket++;
  rw->WR++;
  while (rw->AW > 0 ||
         (rw->WW > 0 && rw->phase == RWM_PHASE_WRITER) ||
         (rw->phase == RWM_PHASE_READER && ticket >= rw->reader_batch_end)) {
    pthread_cond_wait(&rw->can_read, &rw->m);
  }
  rw->WR--;
  rw->AR++;
  if (rw->phase == RWM_PHASE_READER) {
    rw->reader_batch_remaining--;
    if (rw->reader_batch_remaining == 0) {
      /* Close admission; writers still wait for the active readers to leave. */
      rw->phase = RWM_PHASE_WRITER;
      if (rw->WW == 0) pthread_cond_broadcast(&rw->can_read);
    }
  }
  pthread_mutex_unlock(&rw->m);
}

void rwm_runlock(rwlock_monitor_t *rw) {
  pthread_mutex_lock(&rw->m);
  rw->AR--;
  if (rw->AR == 0) {
    if (rw->phase == RWM_PHASE_READER) {
      /* A broadcast reader may not have reacquired the mutex yet. */
      pthread_cond_broadcast(&rw->can_read);
    } else if (rw->WW > 0) {
      pthread_cond_signal(&rw->can_write);
    } else if (rw->WR > 0) {
      pthread_cond_broadcast(&rw->can_read);
    }
  }
  pthread_mutex_unlock(&rw->m);
}

void rwm_wlock(rwlock_monitor_t *rw) {
  pthread_mutex_lock(&rw->m);
  rw->WW++;
  while (rw->AR > 0 || rw->AW > 0 ||
         (rw->phase == RWM_PHASE_READER && rw->reader_batch_remaining > 0)) {
    pthread_cond_wait(&rw->can_write, &rw->m);
  }
  rw->WW--;
  rw->AW = 1;
  if (rw->WR > 0) {
    rw->writer_batch_count++;
  } else {
    rw->writer_batch_count = 0;
  }
  pthread_mutex_unlock(&rw->m);
}

void rwm_wunlock(rwlock_monitor_t *rw) {
  pthread_mutex_lock(&rw->m);
  rw->AW = 0;
  if (rw->WR > 0 &&
      (rw->writer_batch_count >= RWM_WRITER_BATCH || rw->WW == 0)) {
    rw->phase = RWM_PHASE_READER;
    rw->writer_batch_count = 0;
    rw->reader_batch_end = rw->next_reader_ticket;
    rw->reader_batch_remaining = rw->WR;
    pthread_cond_broadcast(&rw->can_read);
  } else {
    rw->phase = RWM_PHASE_WRITER;
    if (rw->WW > 0) {
      pthread_cond_signal(&rw->can_write);
    } else {
      rw->writer_batch_count = 0;
    }
  }
  pthread_mutex_unlock(&rw->m);
}
