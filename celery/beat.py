"""The periodic task scheduler."""

import copy
import dbm
import errno
import heapq
import os
import shelve
import sys
import time
import traceback
from calendar import timegm
from collections import deque, namedtuple
from datetime import timedelta
from functools import total_ordering
from threading import Event, Thread

from billiard import ensure_multiprocessing
from billiard.common import reset_signals
from billiard.context import Process
from kombu.utils.functional import maybe_evaluate, reprcall
from kombu.utils.objects import cached_property

from . import __version__, platforms, signals
from .backends.base import DisabledBackend
from .exceptions import reraise
from .schedules import crontab, maybe_schedule
from .states import READY_STATES
from .utils.functional import is_numeric_value
from .utils.imports import load_extension_class_names, symbol_by_name
from .utils.log import get_logger, iter_open_logger_fds
from .utils.time import humanize_seconds, maybe_make_aware, maybe_timedelta

__all__ = (
    'SchedulingError', 'ScheduleEntry', 'Scheduler',
    'PersistentScheduler', 'Service', 'EmbeddedService',
)

event_t = namedtuple('event_t', ('time', 'priority', 'entry'))

#: Record of a scheduled run that was skipped by the scheduler,
#: see :meth:`Scheduler.skipped_runs`.
skipped_run_t = namedtuple('skipped_run_t', (
    'entry', 'scheduled_at', 'skipped_at', 'reason', 'detail',
    'missed_count',
))

#: Skip reason used when a run is dispatched later than the entry's
#: ``misfire_grace_time`` allows.
SKIP_REASON_MISFIRE = 'misfire'

#: Skip reason used when the previous run of a ``no_overlap`` entry
#: is still active.
SKIP_REASON_OVERLAP = 'overlap'

logger = get_logger(__name__)
debug, info, error, warning = (logger.debug, logger.info,
                               logger.error, logger.warning)

DEFAULT_MAX_INTERVAL = 300  # 5 minutes


class SchedulingError(Exception):
    """An error occurred while scheduling a task."""


class BeatLazyFunc:
    """A lazy function declared in 'beat_schedule' and called before sending to worker.

    Example:

        beat_schedule = {
            'test-every-5-minutes': {
                'task': 'test',
                'schedule': 300,
                'kwargs': {
                    "current": BeatCallBack(datetime.datetime.now)
                }
            }
        }

    """

    def __init__(self, func, *args, **kwargs):
        self._func = func
        self._func_params = {
            "args": args,
            "kwargs": kwargs
        }

    def __call__(self):
        return self.delay()

    def delay(self):
        return self._func(*self._func_params["args"], **self._func_params["kwargs"])


@total_ordering
class ScheduleEntry:
    """An entry in the scheduler.

    Arguments:
        name (str): see :attr:`name`.
        schedule (~celery.schedules.schedule): see :attr:`schedule`.
        args (Tuple): see :attr:`args`.
        kwargs (Dict): see :attr:`kwargs`.
        options (Dict): see :attr:`options`.
        last_run_at (~datetime.datetime): see :attr:`last_run_at`.
        total_run_count (int): see :attr:`total_run_count`.
        relative (bool): Is the time relative to when the server starts?
        no_overlap (bool): see :attr:`no_overlap`.
        misfire_grace_time (float, ~datetime.timedelta):
            see :attr:`misfire_grace_time`.
    """

    #: The task name
    name = None

    #: The schedule (:class:`~celery.schedules.schedule`)
    schedule = None

    #: Positional arguments to apply.
    args = None

    #: Keyword arguments to apply.
    kwargs = None

    #: Task execution options.
    options = None

    #: The time and date of when this task was last scheduled.
    last_run_at = None

    #: The time and date of when the pending run is scheduled to happen.
    #: This is used to detect runs that are late
    #: (see :attr:`misfire_grace_time`).
    next_run_at = None

    #: Total number of times this task has been scheduled.
    total_run_count = 0

    #: Estimated total number of scheduled runs of this task that were
    #: skipped by the scheduler (see :meth:`Scheduler.skipped_runs`).
    total_skip_count = 0

    #: Don't allow overlapping runs of this task: when a new run is due
    #: while the previously dispatched run is still active, the new run
    #: is skipped instead.  Detecting the state of the previous run
    #: requires a result backend to be configured.
    no_overlap = False

    #: Grace time for late runs, as seconds or ~datetime.timedelta.
    #: A run dispatched later than this after its scheduled time is
    #: skipped instead.  The default (``None``) is to always dispatch,
    #: no matter how late the run is.
    misfire_grace_time = None

    #: Id of the task message dispatched for the most recent run.
    last_task_id = None

    def __init__(self, name=None, task=None, last_run_at=None,
                 total_run_count=None, schedule=None, args=(), kwargs=None,
                 options=None, no_overlap=False, misfire_grace_time=None,
                 next_run_at=None, last_task_id=None, total_skip_count=None,
                 relative=False, app=None):
        self.app = app
        self.name = name
        self.task = task
        self.args = args
        self.kwargs = kwargs if kwargs else {}
        self.options = options if options else {}
        self.schedule = maybe_schedule(schedule, relative, app=self.app)
        self.last_run_at = last_run_at or self.default_now()
        self.total_run_count = total_run_count or 0
        self.no_overlap = no_overlap
        self.misfire_grace_time = maybe_timedelta(misfire_grace_time)
        self.next_run_at = next_run_at
        self.last_task_id = last_task_id
        self.total_skip_count = total_skip_count or 0

    def default_now(self):
        return self.schedule.now() if self.schedule else self.app.now()
    _default_now = default_now  # compat

    def _next_instance(self, last_run_at=None):
        """Return new instance, with date and count fields updated."""
        return self.__class__(**dict(
            self,
            last_run_at=last_run_at or self.default_now(),
            total_run_count=self.total_run_count + 1,
        ))
    __next__ = next = _next_instance  # for 2to3

    def __reduce__(self):
        return self.__class__, (
            self.name, self.task, self.last_run_at, self.total_run_count,
            self.schedule, self.args, self.kwargs, self.options,
            self.no_overlap, self.misfire_grace_time, self.next_run_at,
            self.last_task_id, self.total_skip_count,
        )

    def update(self, other):
        """Update values from another entry.

        Will only update "editable" fields:
            ``task``, ``schedule``, ``args``, ``kwargs``, ``options``,
            ``no_overlap``, ``misfire_grace_time``.
        """
        self.__dict__.update({
            'task': other.task, 'schedule': other.schedule,
            'args': other.args, 'kwargs': other.kwargs,
            'options': other.options,
            'no_overlap': other.no_overlap,
            'misfire_grace_time': other.misfire_grace_time,
        })

    def is_due(self):
        """See :meth:`~celery.schedules.schedule.is_due`."""
        return self.schedule.is_due(self.last_run_at)

    def __iter__(self):
        return iter(vars(self).items())

    def __repr__(self):
        return '<{name}: {0.name} {call} {0.schedule}'.format(
            self,
            call=reprcall(self.task, self.args or (), self.kwargs or {}),
            name=type(self).__name__,
        )

    def __lt__(self, other):
        if isinstance(other, ScheduleEntry):
            # How the object is ordered doesn't really matter, as
            # in the scheduler heap, the order is decided by the
            # preceding members of the tuple ``(time, priority, entry)``.
            #
            # If all that's left to order on is the entry then it can
            # just as well be random.
            return id(self) < id(other)
        return NotImplemented

    def editable_fields_equal(self, other):
        for attr in ('task', 'args', 'kwargs', 'options', 'schedule',
                     'no_overlap', 'misfire_grace_time'):
            if getattr(self, attr) != getattr(other, attr):
                return False
        return True

    def __eq__(self, other):
        """Test schedule entries equality.

        Will only compare "editable" fields:
        ``task``, ``schedule``, ``args``, ``kwargs``, ``options``.
        """
        return self.editable_fields_equal(other)


def _evaluate_entry_args(entry_args):
    if not entry_args:
        return []
    return [
        v() if isinstance(v, BeatLazyFunc) else v
        for v in entry_args
    ]


def _evaluate_entry_kwargs(entry_kwargs):
    if not entry_kwargs:
        return {}
    return {
        k: v() if isinstance(v, BeatLazyFunc) else v
        for k, v in entry_kwargs.items()
    }


class Scheduler:
    """Scheduler for periodic tasks.

    The :program:`celery beat` program may instantiate this class
    multiple times for introspection purposes, but then with the
    ``lazy`` argument set.  It's important for subclasses to
    be idempotent when this argument is set.

    Arguments:
        schedule (~celery.schedules.schedule): see :attr:`schedule`.
        max_interval (int): see :attr:`max_interval`.
        lazy (bool): Don't set up the schedule.
    """

    Entry = ScheduleEntry

    #: The schedule dict/shelve.
    schedule = None

    #: Maximum time to sleep between re-checking the schedule.
    max_interval = DEFAULT_MAX_INTERVAL

    #: How often to sync the schedule (3 minutes by default)
    sync_every = 3 * 60

    #: How many tasks can be called before a sync is forced.
    sync_every_tasks = None

    #: Maximum number of skipped-run records kept by the scheduler.
    max_skip_records = 1000

    _last_sync = None
    _tasks_since_sync = 0

    logger = logger  # compat

    def __init__(self, app, schedule=None, max_interval=None,
                 Producer=None, lazy=False, sync_every_tasks=None, **kwargs):
        self.app = app
        self.data = maybe_evaluate({} if schedule is None else schedule)
        self.max_interval = (max_interval or
                             app.conf.beat_max_loop_interval or
                             self.max_interval)
        self.Producer = Producer or app.amqp.Producer
        self._heap = None
        self.old_schedulers = None
        self.sync_every_tasks = (
            app.conf.beat_sync_every if sync_every_tasks is None
            else sync_every_tasks)
        self._skipped_runs = deque(maxlen=self.max_skip_records)
        self._overlap_no_backend_warned = set()
        if not lazy:
            self.setup_schedule()

    def install_default_entries(self, data):
        entries = {}
        if self.app.conf.result_expires and \
                not self.app.backend.supports_autoexpire:
            if 'celery.backend_cleanup' not in data:
                entries['celery.backend_cleanup'] = {
                    'task': 'celery.backend_cleanup',
                    'schedule': crontab('0', '4', '*'),
                    'options': {'expires': 12 * 3600}}
        self.update_from_dict(entries)

    def apply_entry(self, entry, producer=None):
        info('Scheduler: Sending due task %s (%s)', entry.name, entry.task)
        try:
            result = self.apply_async(entry, producer=producer, advance=False)
        except Exception as exc:  # pylint: disable=broad-except
            error('Message Error: %s\n%s',
                  exc, traceback.format_stack(), exc_info=True)
        else:
            if result and hasattr(result, 'id'):
                debug('%s sent. id->%s', entry.task, result.id)
                if entry.no_overlap:
                    # Remember the id of the dispatched run, so that the
                    # next run can be skipped while this one is active.
                    current = self.schedule.get(entry.name)
                    if current is not None:
                        current.last_task_id = result.id
            else:
                debug('%s sent.', entry.task)

    def adjust(self, n, drift=-0.010):
        if n and n > 0:
            return n + drift
        return n

    def is_due(self, entry):
        return entry.is_due()

    def _when(self, entry, next_time_to_run, mktime=timegm):
        """Return a utc timestamp, make sure heapq in correct order."""
        adjust = self.adjust

        as_now = maybe_make_aware(entry.default_now())

        return (mktime(as_now.utctimetuple()) +
                as_now.microsecond / 1e6 +
                (adjust(next_time_to_run) or 0))

    def populate_heap(self, event_t=event_t, heapify=heapq.heapify):
        """Populate the heap with the data contained in the schedule."""
        priority = 5
        self._heap = []
        for entry in self.schedule.values():
            is_due, next_call_delay = entry.is_due()
            self._heap.append(event_t(
                self._when(
                    entry,
                    0 if is_due else next_call_delay
                ) or 0,
                priority, entry
            ))
        heapify(self._heap)

    # pylint disable=redefined-outer-name
    def tick(self, event_t=event_t, min=min, heappop=heapq.heappop,
             heappush=heapq.heappush):
        """Run a tick - one iteration of the scheduler.

        Executes one due task per call.

        Returns:
            float: preferred delay in seconds for next call.
        """
        adjust = self.adjust
        max_interval = self.max_interval

        if (self._heap is None or
                not self.schedules_equal(self.old_schedulers, self.schedule)):
            self.old_schedulers = copy.copy(self.schedule)
            self.populate_heap()

        H = self._heap

        if not H:
            return max_interval

        event = H[0]
        entry = event[2]
        is_due, next_time_to_run = self.is_due(entry)
        if is_due:
            verify = heappop(H)
            if verify is event:
                skip = self._should_skip(entry)
                next_entry = self.reserve(entry)
                next_entry.next_run_at = self._next_run_at(
                    entry, next_time_to_run)
                if skip is None:
                    self.apply_entry(entry, producer=self.producer)
                else:
                    self._record_skip(next_entry, skip)
                heappush(H, event_t(self._when(next_entry, next_time_to_run),
                                    event[1], next_entry))
                return 0
            else:
                heappush(H, verify)
                return min(verify[0], max_interval)
        adjusted_next_time_to_run = adjust(next_time_to_run)
        return min(adjusted_next_time_to_run if is_numeric_value(adjusted_next_time_to_run) else max_interval,
                   max_interval)

    def schedules_equal(self, old_schedules, new_schedules):
        if old_schedules is new_schedules is None:
            return True
        if old_schedules is None or new_schedules is None:
            return False
        if set(old_schedules.keys()) != set(new_schedules.keys()):
            return False
        for name, old_entry in old_schedules.items():
            new_entry = new_schedules.get(name)
            if not new_entry:
                return False
            if new_entry != old_entry:
                return False
        return True

    def should_sync(self):
        return (
            (not self._last_sync or
             (time.monotonic() - self._last_sync) > self.sync_every) or
            (self.sync_every_tasks and
             self._tasks_since_sync >= self.sync_every_tasks)
        )

    def reserve(self, entry):
        new_entry = self.schedule[entry.name] = next(entry)
        return new_entry

    def _next_run_at(self, entry, next_time_to_run):
        """Calculate when the pending run of ``entry`` is scheduled for."""
        if not is_numeric_value(next_time_to_run):
            return None
        return (maybe_make_aware(entry.default_now()) +
                timedelta(seconds=next_time_to_run))

    def _should_skip(self, entry):
        """Check whether the due run of ``entry`` must be skipped.

        Returns:
            skipped_run_t: record describing the skipped run, or
                :const:`None` when the run should be dispatched normally.
        """
        now = maybe_make_aware(entry.default_now())
        return (self._misfire_record(entry, now) or
                self._overlap_record(entry, now))

    def _misfire_record(self, entry, now):
        grace = entry.misfire_grace_time
        if grace is None or entry.next_run_at is None:
            # Either no grace time is configured, or the scheduled time
            # of this run is unknown (new entry, or the schedule was
            # written by an older version): dispatch as usual.
            return None
        scheduled_at = maybe_make_aware(entry.next_run_at)
        delay = (now - scheduled_at).total_seconds()
        if delay <= grace.total_seconds():
            return None
        return skipped_run_t(
            entry.name, scheduled_at, now, SKIP_REASON_MISFIRE,
            'run is {:.1f}s late, exceeding misfire_grace_time of '
            '{:.1f}s'.format(delay, grace.total_seconds()),
            self._estimate_missed(entry, now),
        )

    def _overlap_record(self, entry, now):
        if not entry.no_overlap or not entry.last_task_id:
            return None
        if isinstance(self.app.backend, DisabledBackend):
            if entry.name not in self._overlap_no_backend_warned:
                warning(
                    'beat: Entry %r has no_overlap enabled, but no result '
                    'backend is configured: cannot detect overlapping runs.',
                    entry.name)
                self._overlap_no_backend_warned.add(entry.name)
            return None
        try:
            state = self.app.AsyncResult(entry.last_task_id).state
        except Exception as exc:  # pylint: disable=broad-except
            warning(
                'beat: Cannot check state of task %s for no_overlap '
                'entry %r: %r', entry.last_task_id, entry.name, exc)
            return None
        if state in READY_STATES:
            return None
        return skipped_run_t(
            entry.name, now, now, SKIP_REASON_OVERLAP,
            'previous run still active: task {} in state {}'.format(
                entry.last_task_id, state),
            1,
        )

    def _estimate_missed(self, entry, now):
        """Estimate how many scheduled runs were coalesced into this one.

        Only possible for fixed-interval schedules, returns
        :const:`None` otherwise.
        """
        run_every = getattr(entry.schedule, 'run_every', None)
        if run_every is None or entry.next_run_at is None:
            return None
        interval = run_every.total_seconds()
        if interval <= 0:
            return None
        delay = (now - maybe_make_aware(entry.next_run_at)).total_seconds()
        return max(1, int(delay // interval) + 1)

    def _record_skip(self, entry, record):
        warning('Scheduler: Skipping due task %s (%s): %s',
                entry.name, entry.task, record.detail)
        entry.total_skip_count += record.missed_count or 1
        self._skipped_runs.append(record)
        self._persist_skip(record)

    def _persist_skip(self, record):
        """Store a skipped-run record persistently (no-op by default)."""

    def skipped_runs(self, entry_name=None):
        """Return records of runs skipped by the scheduler.

        Arguments:
            entry_name (str): Optionally, only return records for the
                entry with this name.
        """
        if entry_name is None:
            return list(self._skipped_runs)
        return [r for r in self._skipped_runs if r.entry == entry_name]

    def apply_async(self, entry, producer=None, advance=True, **kwargs):
        # Update time-stamps and run counts before we actually execute,
        # so we have that done if an exception is raised (doesn't schedule
        # forever.)
        entry = self.reserve(entry) if advance else entry
        task = self.app.tasks.get(entry.task)

        try:
            entry_args = _evaluate_entry_args(entry.args)
            entry_kwargs = _evaluate_entry_kwargs(entry.kwargs)
            if task:
                return task.apply_async(entry_args, entry_kwargs,
                                        producer=producer,
                                        **entry.options)
            else:
                return self.send_task(entry.task, entry_args, entry_kwargs,
                                      producer=producer,
                                      **entry.options)
        except Exception as exc:  # pylint: disable=broad-except
            reraise(SchedulingError, SchedulingError(
                "Couldn't apply scheduled task {0.name}: {exc}".format(
                    entry, exc=exc)), sys.exc_info()[2])
        finally:
            self._tasks_since_sync += 1
            if self.should_sync():
                self._do_sync()

    def send_task(self, *args, **kwargs):
        return self.app.send_task(*args, **kwargs)

    def setup_schedule(self):
        self.install_default_entries(self.data)
        self.merge_inplace(self.app.conf.beat_schedule)

    def _do_sync(self):
        try:
            debug('beat: Synchronizing schedule...')
            self.sync()
        finally:
            self._last_sync = time.monotonic()
            self._tasks_since_sync = 0

    def sync(self):
        pass

    def close(self):
        self.sync()

    def add(self, **kwargs):
        entry = self.Entry(app=self.app, **kwargs)
        self.schedule[entry.name] = entry
        return entry

    def _maybe_entry(self, name, entry):
        if isinstance(entry, self.Entry):
            entry.app = self.app
            return entry
        return self.Entry(**dict(entry, name=name, app=self.app))

    def update_from_dict(self, dict_):
        self.schedule.update({
            name: self._maybe_entry(name, entry)
            for name, entry in dict_.items()
        })

    def merge_inplace(self, b):
        schedule = self.schedule
        A, B = set(schedule), set(b)

        # Remove items from disk not in the schedule anymore.
        for key in A ^ B:
            schedule.pop(key, None)

        # Update and add new items in the schedule
        for key in B:
            entry = self.Entry(**dict(b[key], name=key, app=self.app))
            if schedule.get(key):
                schedule[key].update(entry)
            else:
                schedule[key] = entry

    def _ensure_connected(self):
        # callback called for each retry while the connection
        # can't be established.
        def _error_handler(exc, interval):
            error('beat: Connection error: %s. '
                  'Trying again in %s seconds...', exc, interval)

        return self.connection.ensure_connection(
            _error_handler, self.app.conf.broker_connection_max_retries
        )

    def get_schedule(self):
        return self.data

    def set_schedule(self, schedule):
        self.data = schedule
    schedule = property(get_schedule, set_schedule)

    @cached_property
    def connection(self):
        return self.app.connection_for_write()

    @cached_property
    def producer(self):
        return self.Producer(self._ensure_connected(), auto_declare=False)

    @property
    def info(self):
        return ''


class PersistentScheduler(Scheduler):
    """Scheduler backed by :mod:`shelve` database."""

    persistence = shelve
    known_suffixes = ('', '.db', '.dat', '.bak', '.dir')

    _store = None

    def __init__(self, *args, **kwargs):
        self.schedule_filename = kwargs.get('schedule_filename')
        super().__init__(*args, **kwargs)

    def _remove_db(self):
        for suffix in self.known_suffixes:
            with platforms.ignore_errno(errno.ENOENT):
                os.remove(self.schedule_filename + suffix)

    def _open_schedule(self):
        return self.persistence.open(self.schedule_filename, writeback=True)

    def _destroy_open_corrupted_schedule(self, exc):
        error('Removing corrupted schedule file %r: %r',
              self.schedule_filename, exc, exc_info=True)
        self._remove_db()
        return self._open_schedule()

    def setup_schedule(self):
        try:
            self._store = self._open_schedule()
            # In some cases there may be different errors from a storage
            # backend for corrupted files.  Example - DBPageNotFoundError
            # exception from bsddb.  In such case the file will be
            # successfully opened but the error will be raised on first key
            # retrieving.
            self._store.keys()
        except Exception as exc:  # pylint: disable=broad-except
            self._store = self._destroy_open_corrupted_schedule(exc)

        self._create_schedule()

        tz = self.app.conf.timezone
        stored_tz = self._store.get('tz')
        if stored_tz is not None and stored_tz != tz:
            warning('Reset: Timezone changed from %r to %r', stored_tz, tz)
            self._store.clear()   # Timezone changed, reset db!
        utc = self.app.conf.enable_utc
        stored_utc = self._store.get('utc_enabled')
        if stored_utc is not None and stored_utc != utc:
            choices = {True: 'enabled', False: 'disabled'}
            warning('Reset: UTC changed from %s to %s',
                    choices[stored_utc], choices[utc])
            self._store.clear()   # UTC setting changed, reset db!
        entries = self._store.setdefault('entries', {})
        self._skipped_runs.clear()
        self._skipped_runs.extend(self._store.get('skipped_runs') or [])
        self.merge_inplace(self.app.conf.beat_schedule)
        self.install_default_entries(self.schedule)
        self._store.update({
            '__version__': __version__,
            'tz': tz,
            'utc_enabled': utc,
        })
        self.sync()
        debug('Current schedule:\n' + '\n'.join(
            repr(entry) for entry in entries.values()))

    def _create_schedule(self):
        for _ in (1, 2):
            try:
                self._store['entries']
            except (KeyError, UnicodeDecodeError, TypeError):
                # new schedule db
                try:
                    self._store['entries'] = {}
                except (KeyError, UnicodeDecodeError, TypeError) + dbm.error as exc:
                    self._store = self._destroy_open_corrupted_schedule(exc)
                    continue
            else:
                if '__version__' not in self._store:
                    warning('DB Reset: Account for new __version__ field')
                    self._store.clear()   # remove schedule at 2.2.2 upgrade.
                elif 'tz' not in self._store:
                    warning('DB Reset: Account for new tz field')
                    self._store.clear()   # remove schedule at 3.0.8 upgrade
                elif 'utc_enabled' not in self._store:
                    warning('DB Reset: Account for new utc_enabled field')
                    self._store.clear()   # remove schedule at 3.0.9 upgrade
            break

    def get_schedule(self):
        return self._store['entries']

    def set_schedule(self, schedule):
        self._store['entries'] = schedule
    schedule = property(get_schedule, set_schedule)

    def _persist_skip(self, record):
        if self._store is None:
            return
        skipped = self._store.get('skipped_runs') or []
        skipped.append(record)
        if len(skipped) > self.max_skip_records:
            del skipped[:-self.max_skip_records]
        self._store['skipped_runs'] = skipped

    def sync(self):
        if self._store is not None:
            self._store.sync()

    def close(self):
        self.sync()
        self._store.close()

    @property
    def info(self):
        return f'    . db -> {self.schedule_filename}'


class Service:
    """Celery periodic task service."""

    scheduler_cls = PersistentScheduler

    def __init__(self, app, max_interval=None, schedule_filename=None,
                 scheduler_cls=None):
        self.app = app
        self.max_interval = (max_interval or
                             app.conf.beat_max_loop_interval)
        self.scheduler_cls = scheduler_cls or self.scheduler_cls
        self.schedule_filename = (
            schedule_filename or app.conf.beat_schedule_filename)

        self._is_shutdown = Event()
        self._is_stopped = Event()

    def __reduce__(self):
        return self.__class__, (self.max_interval, self.schedule_filename,
                                self.scheduler_cls, self.app)

    def start(self, embedded_process=False):
        info('beat: Starting...')
        debug('beat: Ticking with max interval->%s',
              humanize_seconds(self.scheduler.max_interval))

        signals.beat_init.send(sender=self)
        if embedded_process:
            signals.beat_embedded_init.send(sender=self)
            platforms.set_process_title('celery beat')

        try:
            while not self._is_shutdown.is_set():
                interval = self.scheduler.tick()
                if interval and interval > 0.0:
                    debug('beat: Waking up %s.',
                          humanize_seconds(interval, prefix='in '))
                    time.sleep(interval)
                    if self.scheduler.should_sync():
                        self.scheduler._do_sync()
        except (KeyboardInterrupt, SystemExit):
            self._is_shutdown.set()
        finally:
            self.sync()

    def sync(self):
        self.scheduler.close()
        self._is_stopped.set()

    def stop(self, wait=False):
        info('beat: Shutting down...')
        self._is_shutdown.set()
        wait and self._is_stopped.wait()  # block until shutdown done.

    def get_scheduler(self, lazy=False,
                      extension_namespace='celery.beat_schedulers'):
        filename = self.schedule_filename
        aliases = dict(load_extension_class_names(extension_namespace))
        return symbol_by_name(self.scheduler_cls, aliases=aliases)(
            app=self.app,
            schedule_filename=filename,
            max_interval=self.max_interval,
            lazy=lazy,
        )

    @cached_property
    def scheduler(self):
        return self.get_scheduler()


class _Threaded(Thread):
    """Embedded task scheduler using threading."""

    def __init__(self, app, **kwargs):
        super().__init__()
        self.app = app
        self.service = Service(app, **kwargs)
        self.daemon = True
        self.name = 'Beat'

    def run(self):
        self.app.set_current()
        self.service.start()

    def stop(self):
        self.service.stop(wait=True)


try:
    ensure_multiprocessing()
except NotImplementedError:     # pragma: no cover
    _Process = None
else:
    class _Process(Process):

        def __init__(self, app, **kwargs):
            super().__init__()
            self.app = app
            self.service = Service(app, **kwargs)
            self.name = 'Beat'

        def run(self):
            reset_signals(full=False)
            platforms.close_open_fds([
                sys.__stdin__, sys.__stdout__, sys.__stderr__,
            ] + list(iter_open_logger_fds()))
            self.app.set_default()
            self.app.set_current()
            self.service.start(embedded_process=True)

        def stop(self):
            self.service.stop()
            self.terminate()


def EmbeddedService(app, max_interval=None, **kwargs):
    """Return embedded clock service.

    Arguments:
        thread (bool): Run threaded instead of as a separate process.
            Uses :mod:`multiprocessing` by default, if available.
    """
    if kwargs.pop('thread', False) or _Process is None:
        # Need short max interval to be able to stop thread
        # in reasonable time.
        return _Threaded(app, max_interval=1, **kwargs)
    return _Process(app, max_interval=max_interval, **kwargs)
