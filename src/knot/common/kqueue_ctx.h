/*  Copyright (C) 2021 CZ.NIC, z.s.p.o. <knot-dns@labs.nic.cz>

    This program is free software: you can redistribute it and/or modify
    it under the terms of the GNU General Public License as published by
    the Free Software Foundation, either version 3 of the License, or
    (at your option) any later version.

    This program is distributed in the hope that it will be useful,
    but WITHOUT ANY WARRANTY; without even the implied warranty of
    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
    GNU General Public License for more details.

    You should have received a copy of the GNU General Public License
    along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */

/*!
 * \brief I/O multiplexing with context and timeouts for each fd.
 */

#pragma once

#ifdef USE_KQUEUE

#include <stddef.h>
#include <signal.h>
#include <sys/event.h>
#include <sys/time.h>

#define KQUEUE_CTX_INIT_SIZE 256 /* Resize step. */

/*! \brief Set of filedescriptors with associated context and timeouts. */
typedef struct kqueue_ctx {
    int ctx;
	unsigned n;               /*!< Active fds. */
	unsigned size;            /*!< Array size (allocated). */
	struct kevent *ev;          /*!< Epoll event storage for each fd */
	void* *usrctx;            /*!< Context for each fd. */
	time_t *timeout;          /*!< Timeout for each fd (seconds precision). */
} kqueue_ctx_t;

/*! \brief Mark-and-sweep state. */
enum kqueue_ctx_sweep_state {
	KQUEUE_CTX_KEEP,
	KQUEUE_CTX_SWEEP
};

/*! \brief Sweep callback (set, index, data) */
typedef enum kqueue_ctx_sweep_state (*kqueue_ctx_sweep_cb_t)(kqueue_ctx_t *, int, void*);

/*!
 * \brief Initialize fdset to given size.
 */
int kqueue_ctx_init(kqueue_ctx_t *set, unsigned size);

/*!
 * \brief Destroy FDSET.
 *
 * \retval 0 if successful.
 * \retval -1 on error.
 */
int kqueue_ctx_clear(kqueue_ctx_t* set);

void kqueue_ctx_close(kqueue_ctx_t* set);

/*!
 * \brief Add file descriptor to watched set.
 *
 * \param set Target set.
 * \param fd Added file descriptor.
 * \param events Mask of watched events.
 * \param ctx Context (optional).
 *
 * \retval index of the added fd if successful.
 * \retval -1 on errors.
 */
int kqueue_ctx_add(kqueue_ctx_t *set, int fd, unsigned events, void *ctx);

/*!
 * \brief Remove file descriptor from watched set.
 *
 * \param set Target set.
 * \param i Index of the removed fd.
 *
 * \retval 0 if successful.
 * \retval -1 on errors.
 */
int kqueue_ctx_remove(kqueue_ctx_t *set, unsigned i);

int kqueue_ctx_wait(kqueue_ctx_t *set, struct kevent *ev, size_t offset, size_t ev_size, int timeout);

/*!
 * \brief Set file descriptor watchdog interval.
 *
 * Set time (interval from now) after which the associated file descriptor
 * should be sweeped (see fdset_sweep). Good example is setting a grace period
 * of N seconds between socket activity. If socket is not active within
 * <now, now + interval>, it is sweeped and potentially closed.
 *
 * \param set Target set.
 * \param i Index for the file descriptor.
 * \param interval Allowed interval without activity (seconds).
 *                 -1 disables watchdog timer
 *
 * \retval 0 if successful.
 * \retval -1 on errors.
 */
int kqueue_ctx_set_watchdog(kqueue_ctx_t* set, int i, int interval);

/*!
 * \brief Sweep file descriptors with exceeding inactivity period.
 *
 * \param set Target set.
 * \param cb Callback for sweeped descriptors.
 * \param data Pointer to extra data.
 *
 * \retval number of sweeped descriptors.
 * \retval -1 on errors.
 */
int aio_ctx_sweep(kqueue_ctx_t* set, kqueue_ctx_sweep_cb_t cb, void *data);

#endif