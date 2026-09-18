import dbm
import errno
import sys
from datetime import datetime, timedelta, timezone
from pickle import dumps, loads
from unittest.mock import MagicMock, Mock, call, patch

import pytest

from celery import __version__, beat, uuid
from celery.beat import BeatLazyFunc, event_t
from celery.schedules import crontab, schedule
from celery.utils.objects import Bunch

if sys.version_info >= (3, 9):
    from zoneinfo import ZoneInfo
else:
    from backports.zoneinfo import ZoneInfo


class MockShelve(dict):
    closed = False
    synced = False

    def close(self):
        self.closed = True

    def sync(self):
        self.synced = True


class MockService:
    started = False
    stopped = False

    def __init__(self, *args, **kwargs):
        pass

    def start(self, **kwargs):
        self.started = True

    def stop(self, **kwargs):
        self.stopped = True


class test_BeatLazyFunc:

    def test_beat_lazy_func(self):
        def add(a, b):
            return a + b
        result = BeatLazyFunc(add, 1, 2)
        assert add(1, 2) == result()
        assert add(1, 2) == result.delay()


class test_ScheduleEntry:
    Entry = beat.ScheduleEntry

    def create_entry(self, **kwargs):
        entry = {
            'name': 'celery.unittest.add',
            'schedule': timedelta(seconds=10),
            'args': (2, 2),
            'options': {'routing_key': 'cpu'},
            'app': self.app,
        }
        return self.Entry(**dict(entry, **kwargs))

    def test_next(self):
        entry = self.create_entry(schedule=10)
        assert entry.last_run_at
        assert isinstance(entry.last_run_at, datetime)
        assert entry.total_run_count == 0

        next_run_at = entry.last_run_at + timedelta(seconds=10)
        next_entry = entry.next(next_run_at)
        assert next_entry.last_run_at >= next_run_at
        assert next_entry.total_run_count == 1

    def test_is_due(self):
        entry = self.create_entry(schedule=timedelta(seconds=10))
        assert entry.app is self.app
        assert entry.schedule.app is self.app
        due1, next_time_to_run1 = entry.is_due()
        assert not due1
        assert next_time_to_run1 > 9

        next_run_at = entry.last_run_at - timedelta(seconds=10)
        next_entry = entry.next(next_run_at)
        due2, next_time_to_run2 = next_entry.is_due()
        assert due2
        assert next_time_to_run2 > 9

    def test_repr(self):
        entry = self.create_entry()
        assert '<ScheduleEntry:' in repr(entry)

    def test_reduce(self):
        entry = self.create_entry(schedule=timedelta(seconds=10))
        fun, args = entry.__reduce__()
        res = fun(*args)
        assert res.schedule == entry.schedule

    def test_lt(self):
        e1 = self.create_entry(schedule=timedelta(seconds=10))
        e2 = self.create_entry(schedule=timedelta(seconds=2))
        # order doesn't matter, see comment in __lt__
        res1 = e1 < e2  # noqa
        try:
            res2 = e1 < object()  # noqa
        except TypeError:
            pass

    def test_update(self):
        entry = self.create_entry()
        assert entry.schedule == timedelta(seconds=10)
        assert entry.args == (2, 2)
        assert entry.kwargs == {}
        assert entry.options == {'routing_key': 'cpu'}

        entry2 = self.create_entry(schedule=timedelta(minutes=20),
                                   args=(16, 16),
                                   kwargs={'callback': 'foo.bar.baz'},
                                   options={'routing_key': 'urgent'})
        entry.update(entry2)
        assert entry.schedule == schedule(timedelta(minutes=20))
        assert entry.args == (16, 16)
        assert entry.kwargs == {'callback': 'foo.bar.baz'}
        assert entry.options == {'routing_key': 'urgent'}

    def test_skip_control_defaults(self):
        entry = self.create_entry()
        assert entry.no_overlap is False
        assert entry.misfire_grace_time is None
        assert entry.next_run_at is None
        assert entry.last_task_id is None
        assert entry.total_skip_count == 0

    def test_misfire_grace_time_coerced_to_timedelta(self):
        entry = self.create_entry(misfire_grace_time=30)
        assert entry.misfire_grace_time == timedelta(seconds=30)

    def test_next_preserves_skip_control_fields(self):
        next_run_at = datetime.now(timezone.utc)
        entry = self.create_entry(no_overlap=True, misfire_grace_time=30,
                                  next_run_at=next_run_at,
                                  last_task_id='task-id-1',
                                  total_skip_count=3)
        next_entry = entry.next()
        assert next_entry.no_overlap is True
        assert next_entry.misfire_grace_time == timedelta(seconds=30)
        assert next_entry.next_run_at == next_run_at
        assert next_entry.last_task_id == 'task-id-1'
        assert next_entry.total_skip_count == 3

    def test_reduce_preserves_skip_control_fields(self):
        next_run_at = datetime.now(timezone.utc)
        entry = self.create_entry(no_overlap=True,
                                  misfire_grace_time=timedelta(seconds=5),
                                  next_run_at=next_run_at,
                                  last_task_id='task-id-1',
                                  total_skip_count=3)
        res = loads(dumps(entry))
        assert res.no_overlap is True
        assert res.misfire_grace_time == timedelta(seconds=5)
        assert res.next_run_at == next_run_at
        assert res.last_task_id == 'task-id-1'
        assert res.total_skip_count == 3

    def test_update_copies_skip_control_fields(self):
        entry = self.create_entry()
        entry2 = self.create_entry(no_overlap=True, misfire_grace_time=10)
        entry.update(entry2)
        assert entry.no_overlap is True
        assert entry.misfire_grace_time == timedelta(seconds=10)

    def test_editable_fields_equal_skip_control(self):
        entry = self.create_entry()
        other = self.create_entry()
        assert entry.editable_fields_equal(other)
        assert entry == other
        other.no_overlap = True
        assert not entry.editable_fields_equal(other)
        other.no_overlap = False
        other.misfire_grace_time = timedelta(seconds=10)
        assert not entry.editable_fields_equal(other)


class mScheduler(beat.Scheduler):

    def __init__(self, *args, **kwargs):
        self.sent = []
        super().__init__(*args, **kwargs)

    def send_task(self, name=None, args=None, kwargs=None, **options):
        self.sent.append({'name': name,
                          'args': args,
                          'kwargs': kwargs,
                          'options': options})
        return self.app.AsyncResult(uuid())


class mSchedulerSchedulingError(mScheduler):

    def send_task(self, *args, **kwargs):
        raise beat.SchedulingError('Could not apply task')


class mSchedulerRuntimeError(mScheduler):

    def is_due(self, *args, **kwargs):
        raise RuntimeError('dict modified while itervalues')


class mocked_schedule(schedule):

    def now_func():
        return datetime.now(timezone.utc)

    def __init__(self, is_due, next_run_at, nowfun=now_func):
        self._is_due = is_due
        self._next_run_at = next_run_at
        self.run_every = timedelta(seconds=1)
        self.nowfun = nowfun
        self.default_now = self.nowfun

    def is_due(self, last_run_at):
        return self._is_due, self._next_run_at


always_due = mocked_schedule(True, 1)
always_pending = mocked_schedule(False, 1)
always_pending_left_10_milliseconds = mocked_schedule(False, 0.01)


class test_Scheduler:

    def test_custom_schedule_dict(self):
        custom = {'foo': 'bar'}
        scheduler = mScheduler(app=self.app, schedule=custom, lazy=True)
        assert scheduler.data is custom

    def test_apply_async_uses_registered_task_instances(self):

        @self.app.task(shared=False)
        def foo():
            pass
        foo.apply_async = Mock(name='foo.apply_async')
        assert foo.name in foo._get_app().tasks

        scheduler = mScheduler(app=self.app)
        scheduler.apply_async(scheduler.Entry(task=foo.name, app=self.app))
        foo.apply_async.assert_called()

    def test_apply_async_with_null_args(self):

        @self.app.task(shared=False)
        def foo():
            pass
        foo.apply_async = Mock(name='foo.apply_async')

        scheduler = mScheduler(app=self.app)
        scheduler.apply_async(
            scheduler.Entry(
                task=foo.name, app=self.app, args=None, kwargs=None))
        foo.apply_async.assert_called()

    def test_apply_async_with_null_args_set_to_none(self):

        @self.app.task(shared=False)
        def foo():
            pass
        foo.apply_async = Mock(name='foo.apply_async')

        scheduler = mScheduler(app=self.app)
        entry = scheduler.Entry(task=foo.name, app=self.app, args=None,
                                kwargs=None)
        entry.args = None
        entry.kwargs = None

        scheduler.apply_async(entry, advance=False)
        foo.apply_async.assert_called()

    def test_apply_async_without_null_args(self):

        @self.app.task(shared=False)
        def foo(moo: int):
            return moo
        foo.apply_async = Mock(name='foo.apply_async')

        scheduler = mScheduler(app=self.app)
        entry = scheduler.Entry(task=foo.name, app=self.app, args=None,
                                kwargs=None)
        entry.args = (101,)
        entry.kwargs = None

        scheduler.apply_async(entry, advance=False)
        foo.apply_async.assert_called()
        assert foo.apply_async.call_args[0][0] == [101]

    def test_should_sync(self):

        @self.app.task(shared=False)
        def not_sync():
            pass
        not_sync.apply_async = Mock()

        s = mScheduler(app=self.app)
        s._do_sync = Mock()
        s.should_sync = Mock()
        s.should_sync.return_value = True
        s.apply_async(s.Entry(task=not_sync.name, app=self.app))
        s._do_sync.assert_called_with()

        s._do_sync = Mock()
        s.should_sync.return_value = False
        s.apply_async(s.Entry(task=not_sync.name, app=self.app))
        s._do_sync.assert_not_called()

    def test_should_sync_increments_sync_every_counter(self):
        self.app.conf.beat_sync_every = 2

        @self.app.task(shared=False)
        def not_sync():
            pass
        not_sync.apply_async = Mock()

        s = mScheduler(app=self.app)
        assert s.sync_every_tasks == 2
        s._do_sync = Mock()

        s.apply_async(s.Entry(task=not_sync.name, app=self.app))
        assert s._tasks_since_sync == 1
        s.apply_async(s.Entry(task=not_sync.name, app=self.app))
        s._do_sync.assert_called_with()

        self.app.conf.beat_sync_every = 0

    def test_sync_task_counter_resets_on_do_sync(self):
        self.app.conf.beat_sync_every = 1

        @self.app.task(shared=False)
        def not_sync():
            pass
        not_sync.apply_async = Mock()

        s = mScheduler(app=self.app)
        assert s.sync_every_tasks == 1

        s.apply_async(s.Entry(task=not_sync.name, app=self.app))
        assert s._tasks_since_sync == 0

        self.app.conf.beat_sync_every = 0

    @patch('celery.app.base.Celery.send_task')
    def test_send_task(self, send_task):
        b = beat.Scheduler(app=self.app)
        b.send_task('tasks.add', countdown=10)
        send_task.assert_called_with('tasks.add', countdown=10)

    def test_info(self):
        scheduler = mScheduler(app=self.app)
        assert isinstance(scheduler.info, str)

    def test_apply_entry_handles_empty_result(self):
        s = mScheduler(app=self.app)
        entry = s.Entry(name='a name', task='foo', app=self.app)

        with patch.object(s, 'apply_async') as mock_apply_async:
            with patch("celery.beat.debug") as mock_debug:
                mock_apply_async.return_value = None
                s.apply_entry(entry)
        mock_debug.assert_called_once_with('%s sent.', entry.task)

        with patch.object(s, 'apply_async') as mock_apply_async:
            with patch("celery.beat.debug") as mock_debug:
                mock_apply_async.return_value = object()
                s.apply_entry(entry)
        mock_debug.assert_called_once_with('%s sent.', entry.task)

        task_id = 'taskId123456'
        with patch.object(s, 'apply_async') as mock_apply_async:
            with patch("celery.beat.debug") as mock_debug:
                mock_apply_async.return_value = self.app.AsyncResult(task_id)
                s.apply_entry(entry)
        mock_debug.assert_called_once_with('%s sent. id->%s', entry.task, task_id)

    def test_maybe_entry(self):
        s = mScheduler(app=self.app)
        entry = s.Entry(name='add every', task='tasks.add', app=self.app)
        assert s._maybe_entry(entry.name, entry) is entry
        assert s._maybe_entry('add every', {'task': 'tasks.add'})

    def test_set_schedule(self):
        s = mScheduler(app=self.app)
        s.schedule = {'foo': 'bar'}
        assert s.data == {'foo': 'bar'}

    @patch('kombu.connection.Connection.ensure_connection')
    def test_ensure_connection_error_handler(self, ensure):
        s = mScheduler(app=self.app)
        assert s._ensure_connected()
        ensure.assert_called()
        callback = ensure.call_args[0][0]

        callback(KeyError(), 5)

    def test_install_default_entries(self):
        self.app.conf.result_expires = None
        self.app.conf.beat_schedule = {}
        s = mScheduler(app=self.app)
        s.install_default_entries({})
        assert 'celery.backend_cleanup' not in s.data
        self.app.backend.supports_autoexpire = False

        self.app.conf.result_expires = 30
        s = mScheduler(app=self.app)
        s.install_default_entries({})
        assert 'celery.backend_cleanup' in s.data

        self.app.backend.supports_autoexpire = True
        self.app.conf.result_expires = 31
        s = mScheduler(app=self.app)
        s.install_default_entries({})
        assert 'celery.backend_cleanup' not in s.data

    def test_due_tick(self):
        scheduler = mScheduler(app=self.app)
        scheduler.add(name='test_due_tick',
                      schedule=always_due,
                      args=(1, 2),
                      kwargs={'foo': 'bar'})
        assert scheduler.tick() == 0

    @patch('celery.beat.error')
    def test_due_tick_SchedulingError(self, error):
        scheduler = mSchedulerSchedulingError(app=self.app)
        scheduler.add(name='test_due_tick_SchedulingError',
                      schedule=always_due)
        assert scheduler.tick() == 0
        error.assert_called()

    def test_pending_tick(self):
        scheduler = mScheduler(app=self.app)
        scheduler.add(name='test_pending_tick',
                      schedule=always_pending)
        assert scheduler.tick() == 1 - 0.010

    def test_pending_left_10_milliseconds_tick(self):
        scheduler = mScheduler(app=self.app)
        scheduler.add(name='test_pending_left_10_milliseconds_tick',
                      schedule=always_pending_left_10_milliseconds)
        assert scheduler.tick() == 0.010 - 0.010

    def test_honors_max_interval(self):
        scheduler = mScheduler(app=self.app)
        maxi = scheduler.max_interval
        scheduler.add(name='test_honors_max_interval',
                      schedule=mocked_schedule(False, maxi * 4))
        assert scheduler.tick() == maxi

    def test_ticks(self):
        scheduler = mScheduler(app=self.app)
        nums = [600, 300, 650, 120, 250, 36]
        s = {'test_ticks%s' % i: {'schedule': mocked_schedule(False, j)}
             for i, j in enumerate(nums)}
        scheduler.update_from_dict(s)
        assert scheduler.tick() == min(nums) - 0.010

    def test_ticks_microseconds(self):
        scheduler = mScheduler(app=self.app)

        now_ts = 1514797200.2
        now = datetime.utcfromtimestamp(now_ts)
        schedule_half = schedule(timedelta(seconds=0.5), nowfun=lambda: now)
        scheduler.add(name='half_second_schedule', schedule=schedule_half)

        scheduler.tick()
        # ensure those 0.2 seconds on now_ts don't get dropped
        expected_time = now_ts + 0.5 - 0.010
        assert scheduler._heap[0].time == expected_time

    def test_ticks_schedule_change(self):
        # initialise schedule and check heap is not initialized
        scheduler = mScheduler(app=self.app)
        assert scheduler._heap is None

        # set initial schedule and check heap is updated
        schedule_5 = schedule(5)
        scheduler.add(name='test_schedule', schedule=schedule_5)
        scheduler.tick()
        assert scheduler._heap[0].entry.schedule == schedule_5

        # update schedule and check heap is updated
        schedule_10 = schedule(10)
        scheduler.add(name='test_schedule', schedule=schedule(10))
        scheduler.tick()
        assert scheduler._heap[0].entry.schedule == schedule_10

    def test_schedule_no_remain(self):
        scheduler = mScheduler(app=self.app)
        scheduler.add(name='test_schedule_no_remain',
                      schedule=mocked_schedule(False, None))
        assert scheduler.tick() == scheduler.max_interval

    def test_interface(self):
        scheduler = mScheduler(app=self.app)
        scheduler.sync()
        scheduler.setup_schedule()
        scheduler.close()

    def test_merge_inplace(self):
        a = mScheduler(app=self.app)
        b = mScheduler(app=self.app)
        a.update_from_dict({'foo': {'schedule': mocked_schedule(True, 10)},
                            'bar': {'schedule': mocked_schedule(True, 20)}})
        b.update_from_dict({'bar': {'schedule': mocked_schedule(True, 40)},
                            'baz': {'schedule': mocked_schedule(True, 10)}})
        a.merge_inplace(b.schedule)

        assert 'foo' not in a.schedule
        assert 'baz' in a.schedule
        assert a.schedule['bar'].schedule._next_run_at == 40

    def test_when(self):
        now_time_utc = datetime(2000, 10, 10, 10, 10,
                                10, 10, tzinfo=ZoneInfo("UTC"))
        now_time_casey = now_time_utc.astimezone(
            ZoneInfo('Antarctica/Casey')
        )
        scheduler = mScheduler(app=self.app)
        result_utc = scheduler._when(
            mocked_schedule(True, 10, lambda: now_time_utc),
            10
        )
        result_casey = scheduler._when(
            mocked_schedule(True, 10, lambda: now_time_casey),
            10
        )
        assert result_utc == result_casey

    @patch('celery.beat.Scheduler._when', return_value=1)
    def test_populate_heap(self, _when):
        scheduler = mScheduler(app=self.app)
        scheduler.update_from_dict(
            {'foo': {'schedule': mocked_schedule(True, 10)}}
        )
        scheduler.populate_heap()
        assert scheduler._heap == [event_t(1, 5, scheduler.schedule['foo'])]

    def create_schedule_entry(self, schedule=None, args=(), kwargs={},
                              options={}, task=None):
        entry = {
            'name': 'celery.unittest.add',
            'schedule': schedule,
            'app': self.app,
            'args': args,
            'kwargs': kwargs,
            'options': options,
            'task': task
        }
        return beat.ScheduleEntry(**dict(entry))

    def test_schedule_equal_schedule_vs_schedule_success(self):
        scheduler = beat.Scheduler(app=self.app)
        a = {'a': self.create_schedule_entry(schedule=schedule(5))}
        b = {'a': self.create_schedule_entry(schedule=schedule(5))}
        assert scheduler.schedules_equal(a, b)

    def test_schedule_equal_schedule_vs_schedule_fail(self):
        scheduler = beat.Scheduler(app=self.app)
        a = {'a': self.create_schedule_entry(schedule=schedule(5))}
        b = {'a': self.create_schedule_entry(schedule=schedule(10))}
        assert not scheduler.schedules_equal(a, b)

    def test_schedule_equal_crontab_vs_crontab_success(self):
        scheduler = beat.Scheduler(app=self.app)
        a = {'a': self.create_schedule_entry(schedule=crontab(minute=5))}
        b = {'a': self.create_schedule_entry(schedule=crontab(minute=5))}
        assert scheduler.schedules_equal(a, b)

    def test_schedule_equal_crontab_vs_crontab_fail(self):
        scheduler = beat.Scheduler(app=self.app)
        a = {'a': self.create_schedule_entry(schedule=crontab(minute=5))}
        b = {'a': self.create_schedule_entry(schedule=crontab(minute=10))}
        assert not scheduler.schedules_equal(a, b)

    def test_schedule_equal_crontab_vs_schedule_fail(self):
        scheduler = beat.Scheduler(app=self.app)
        a = {'a': self.create_schedule_entry(schedule=crontab(minute=5))}
        b = {'a': self.create_schedule_entry(schedule=schedule(5))}
        assert not scheduler.schedules_equal(a, b)

    def test_schedule_equal_different_key_fail(self):
        scheduler = beat.Scheduler(app=self.app)
        a = {'a': self.create_schedule_entry(schedule=schedule(5))}
        b = {'b': self.create_schedule_entry(schedule=schedule(5))}
        assert not scheduler.schedules_equal(a, b)

    def test_schedule_equal_args_vs_args_success(self):
        scheduler = beat.Scheduler(app=self.app)
        a = {'a': self.create_schedule_entry(args='a')}
        b = {'a': self.create_schedule_entry(args='a')}
        assert scheduler.schedules_equal(a, b)

    def test_schedule_equal_args_vs_args_fail(self):
        scheduler = beat.Scheduler(app=self.app)
        a = {'a': self.create_schedule_entry(args='a')}
        b = {'a': self.create_schedule_entry(args='b')}
        assert not scheduler.schedules_equal(a, b)

    def test_schedule_equal_kwargs_vs_kwargs_success(self):
        scheduler = beat.Scheduler(app=self.app)
        a = {'a': self.create_schedule_entry(kwargs={'a': 'a'})}
        b = {'a': self.create_schedule_entry(kwargs={'a': 'a'})}
        assert scheduler.schedules_equal(a, b)

    def test_schedule_equal_kwargs_vs_kwargs_fail(self):
        scheduler = beat.Scheduler(app=self.app)
        a = {'a': self.create_schedule_entry(kwargs={'a': 'a'})}
        b = {'a': self.create_schedule_entry(kwargs={'b': 'b'})}
        assert not scheduler.schedules_equal(a, b)

    def test_schedule_equal_options_vs_options_success(self):
        scheduler = beat.Scheduler(app=self.app)
        a = {'a': self.create_schedule_entry(options={'a': 'a'})}
        b = {'a': self.create_schedule_entry(options={'a': 'a'})}
        assert scheduler.schedules_equal(a, b)

    def test_schedule_equal_options_vs_options_fail(self):
        scheduler = beat.Scheduler(app=self.app)
        a = {'a': self.create_schedule_entry(options={'a': 'a'})}
        b = {'a': self.create_schedule_entry(options={'b': 'b'})}
        assert not scheduler.schedules_equal(a, b)

    def test_schedule_equal_task_vs_task_success(self):
        scheduler = beat.Scheduler(app=self.app)
        a = {'a': self.create_schedule_entry(task='a')}
        b = {'a': self.create_schedule_entry(task='a')}
        assert scheduler.schedules_equal(a, b)

    def test_schedule_equal_task_vs_task_fail(self):
        scheduler = beat.Scheduler(app=self.app)
        a = {'a': self.create_schedule_entry(task='a')}
        b = {'a': self.create_schedule_entry(task='b')}
        assert not scheduler.schedules_equal(a, b)

    def test_schedule_equal_none_entry_vs_entry(self):
        scheduler = beat.Scheduler(app=self.app)
        a = None
        b = {'a': self.create_schedule_entry(task='b')}
        assert not scheduler.schedules_equal(a, b)

    def test_schedule_equal_entry_vs_none_entry(self):
        scheduler = beat.Scheduler(app=self.app)
        a = {'a': self.create_schedule_entry(task='a')}
        b = None
        assert not scheduler.schedules_equal(a, b)

    def test_schedule_equal_none_entry_vs_none_entry(self):
        scheduler = beat.Scheduler(app=self.app)
        a = None
        b = None
        assert scheduler.schedules_equal(a, b)


class test_Scheduler_skip_control:

    def add_entry(self, scheduler, name, now, next_delay=300, **fields):
        sched = mocked_schedule(True, next_delay, nowfun=lambda: now)
        entry = scheduler.add(name=name, task='tasks.add',
                              schedule=sched, **fields)
        return entry, sched

    def test_misfire_beyond_grace_time_is_skipped(self):
        scheduler = mScheduler(app=self.app)
        now = datetime.now(timezone.utc)
        entry, sched = self.add_entry(scheduler, 'late', now,
                                      misfire_grace_time=60)
        sched.run_every = timedelta(seconds=600)
        entry.next_run_at = now - timedelta(seconds=3600)

        assert scheduler.tick() == 0
        assert not scheduler.sent

        skipped = scheduler.skipped_runs()
        assert len(skipped) == 1
        record = skipped[0]
        assert record.entry == 'late'
        assert record.reason == beat.SKIP_REASON_MISFIRE
        assert record.scheduled_at == now - timedelta(seconds=3600)
        assert record.skipped_at == now
        assert record.missed_count == 7

        # the schedule moves on: the entry was reserved and the
        # estimated number of missed runs was recorded.
        current = scheduler.schedule['late']
        assert current.total_run_count == 1
        assert current.total_skip_count == 7
        assert current.last_run_at == now
        assert current.next_run_at == now + timedelta(seconds=300)

        # the next run is on time again and dispatched normally.
        assert scheduler.tick() == 0
        assert len(scheduler.sent) == 1

    def test_misfire_within_grace_time_is_sent(self):
        scheduler = mScheduler(app=self.app)
        now = datetime.now(timezone.utc)
        entry, _ = self.add_entry(scheduler, 'slightly-late', now,
                                  misfire_grace_time=3600)
        entry.next_run_at = now - timedelta(seconds=60)

        assert scheduler.tick() == 0
        assert len(scheduler.sent) == 1
        assert not scheduler.skipped_runs()

    def test_misfire_without_next_run_at_is_sent(self):
        # the scheduled time of the run is unknown (e.g. the schedule
        # was written by an older version): dispatch as usual.
        scheduler = mScheduler(app=self.app)
        now = datetime.now(timezone.utc)
        entry, _ = self.add_entry(scheduler, 'unknown-scheduled-at', now,
                                  misfire_grace_time=1)
        assert entry.next_run_at is None

        assert scheduler.tick() == 0
        assert len(scheduler.sent) == 1
        assert not scheduler.skipped_runs()

    def test_tick_records_next_run_at(self):
        scheduler = mScheduler(app=self.app)
        now = datetime.now(timezone.utc)
        self.add_entry(scheduler, 'entry', now)

        assert scheduler.tick() == 0
        current = scheduler.schedule['entry']
        assert current.next_run_at == now + timedelta(seconds=300)

    def test_no_overlap_skips_while_previous_run_active(self):
        scheduler = mScheduler(app=self.app)
        now = datetime.now(timezone.utc)
        self.add_entry(scheduler, 'serial', now, no_overlap=True)

        # first run is dispatched and its task id is tracked.
        assert scheduler.tick() == 0
        assert len(scheduler.sent) == 1
        current = scheduler.schedule['serial']
        assert current.last_task_id

        # the previous run is still active: the next run is skipped.
        self.app.backend.store_result(current.last_task_id, None, 'STARTED')
        assert scheduler.tick() == 0
        assert len(scheduler.sent) == 1

        skipped = scheduler.skipped_runs()
        assert len(skipped) == 1
        record = skipped[0]
        assert record.entry == 'serial'
        assert record.reason == beat.SKIP_REASON_OVERLAP
        assert record.missed_count == 1
        assert scheduler.schedule['serial'].total_skip_count == 1

    def test_no_overlap_sends_when_previous_run_finished(self):
        scheduler = mScheduler(app=self.app)
        now = datetime.now(timezone.utc)
        self.add_entry(scheduler, 'serial', now, no_overlap=True)

        assert scheduler.tick() == 0
        current = scheduler.schedule['serial']
        self.app.backend.store_result(
            current.last_task_id, 'result', 'SUCCESS')

        assert scheduler.tick() == 0
        assert len(scheduler.sent) == 2
        assert not scheduler.skipped_runs()

    def test_no_overlap_without_backend_sends_and_warns(self):
        self.app.conf.result_backend = 'disabled'
        scheduler = mScheduler(app=self.app)
        now = datetime.now(timezone.utc)
        self.add_entry(scheduler, 'serial', now, no_overlap=True)

        assert scheduler.tick() == 0
        assert scheduler.schedule['serial'].last_task_id

        with patch('celery.beat.warning') as warning:
            assert scheduler.tick() == 0
            warning.assert_called_once()
        assert len(scheduler.sent) == 2

        # the warning is only emitted once per entry.
        with patch('celery.beat.warning') as warning:
            assert scheduler.tick() == 0
            warning.assert_not_called()
        assert len(scheduler.sent) == 3

    def test_skipped_runs_filtered_by_entry_name(self):
        scheduler = mScheduler(app=self.app)
        now = datetime.now(timezone.utc)
        for name in ('a', 'b'):
            entry, _ = self.add_entry(scheduler, name, now,
                                      misfire_grace_time=60)
            entry.next_run_at = now - timedelta(seconds=3600)

        scheduler.tick()
        scheduler.tick()
        assert not scheduler.sent
        assert len(scheduler.skipped_runs()) == 2
        a_skips = scheduler.skipped_runs('a')
        assert len(a_skips) == 1
        assert a_skips[0].entry == 'a'
        assert not scheduler.skipped_runs('no-such-entry')


def create_persistent_scheduler(shelv=None):
    if shelv is None:
        shelv = MockShelve()

    class MockPersistentScheduler(beat.PersistentScheduler):
        sh = shelv
        persistence = Bunch(
            open=lambda *a, **kw: shelv,
        )
        tick_raises_exit = False
        shutdown_service = None

        def tick(self):
            if self.tick_raises_exit:
                raise SystemExit()
            if self.shutdown_service:
                self.shutdown_service._is_shutdown.set()
            return 0.0

    return MockPersistentScheduler, shelv


def create_persistent_scheduler_w_call_logging(shelv=None):
    if shelv is None:
        shelv = MockShelve()

    class MockPersistentScheduler(beat.PersistentScheduler):
        sh = shelv
        persistence = Bunch(
            open=lambda *a, **kw: shelv,
        )

        def __init__(self, *args, **kwargs):
            self.sent = []
            super().__init__(*args, **kwargs)

        def send_task(self, task=None, args=None, kwargs=None, **options):
            self.sent.append({'task': task,
                              'args': args,
                              'kwargs': kwargs,
                              'options': options})
            return self.app.AsyncResult(uuid())
    return MockPersistentScheduler, shelv


class test_PersistentScheduler:

    @patch('os.remove')
    def test_remove_db(self, remove):
        s = create_persistent_scheduler()[0](app=self.app,
                                             schedule_filename='schedule')
        s._remove_db()
        remove.assert_has_calls(
            [call('schedule' + suffix) for suffix in s.known_suffixes]
        )
        err = OSError()
        err.errno = errno.ENOENT
        remove.side_effect = err
        s._remove_db()
        err.errno = errno.EPERM
        with pytest.raises(OSError):
            s._remove_db()

    def test_create_schedule_corrupted(self):
        """
        Test that any decoding errors that might happen when opening beat-schedule.db are caught
        """
        s = create_persistent_scheduler()[0](app=self.app,
                                             schedule_filename='schedule')
        s._store = MagicMock()
        s._destroy_open_corrupted_schedule = Mock()
        s._destroy_open_corrupted_schedule.return_value = MagicMock()

        # self._store['entries'] will throw a KeyError
        s._store.__getitem__.side_effect = KeyError()
        # then, when _create_schedule tries to reset _store['entries'], throw another error
        expected_error = UnicodeDecodeError("ascii", b"ordinal not in range(128)", 0, 0, "")
        s._store.__setitem__.side_effect = expected_error

        s._create_schedule()
        s._destroy_open_corrupted_schedule.assert_called_with(expected_error)

    def test_create_schedule_corrupted_dbm_error(self):
        """
        Test that any dbm.error that might happen when opening beat-schedule.db are caught
        """
        s = create_persistent_scheduler()[0](app=self.app,
                                             schedule_filename='schedule')
        s._store = MagicMock()
        s._destroy_open_corrupted_schedule = Mock()
        s._destroy_open_corrupted_schedule.return_value = MagicMock()

        # self._store['entries'] = {} will throw a KeyError
        s._store.__getitem__.side_effect = KeyError()
        # then, when _create_schedule tries to reset _store['entries'], throw another error, specifically dbm.error
        expected_error = dbm.error[0]()
        s._store.__setitem__.side_effect = expected_error

        s._create_schedule()
        s._destroy_open_corrupted_schedule.assert_called_with(expected_error)

    def test_create_schedule_missing_entries(self):
        """
        Test that if _create_schedule can't find the key "entries" in _store it will recreate it
        """
        s = create_persistent_scheduler()[0](app=self.app, schedule_filename="schedule")
        s._store = MagicMock()

        # self._store['entries'] will throw a KeyError
        s._store.__getitem__.side_effect = TypeError()

        s._create_schedule()
        s._store.__setitem__.assert_called_with("entries", {})

    def test_setup_schedule(self):
        s = create_persistent_scheduler()[0](app=self.app,
                                             schedule_filename='schedule')
        opens = s.persistence.open = Mock()
        s._remove_db = Mock()

        def effect(*args, **kwargs):
            if opens.call_count > 1:
                return s.sh
            raise OSError()
        opens.side_effect = effect
        s.setup_schedule()
        s._remove_db.assert_called_with()

        s._store = {'__version__': 1}
        s.setup_schedule()

        s._store.clear = Mock()
        op = s.persistence.open = Mock()
        op.return_value = s._store
        s._store['tz'] = 'FUNKY'
        s.setup_schedule()
        op.assert_called_with(s.schedule_filename, writeback=True)
        s._store.clear.assert_called_with()
        s._store['utc_enabled'] = False
        s._store.clear = Mock()
        s.setup_schedule()
        s._store.clear.assert_called_with()

    def test_get_schedule(self):
        s = create_persistent_scheduler()[0](
            schedule_filename='schedule', app=self.app,
        )
        s._store = {'entries': {}}
        s.schedule = {'foo': 'bar'}
        assert s.schedule == {'foo': 'bar'}
        assert s._store['entries'] == s.schedule

    def test_run_all_due_tasks_after_restart(self):
        scheduler_class, shelve = create_persistent_scheduler_w_call_logging()

        shelve['tz'] = 'UTC'
        shelve['utc_enabled'] = True
        shelve['__version__'] = __version__
        cur_seconds = 20

        def now_func():
            return datetime(2018, 1, 1, 1, 11, cur_seconds)
        app_schedule = {
            'first_missed': {'schedule': crontab(
                minute='*/10', nowfun=now_func), 'task': 'first_missed'},
            'second_missed': {'schedule': crontab(
                minute='*/1', nowfun=now_func), 'task': 'second_missed'},
            'non_missed': {'schedule': crontab(
                minute='*/13', nowfun=now_func), 'task': 'non_missed'}
        }
        shelve['entries'] = {
            'first_missed': beat.ScheduleEntry(
                'first_missed', 'first_missed',
                last_run_at=now_func() - timedelta(minutes=2),
                total_run_count=10,
                app=self.app,
                schedule=app_schedule['first_missed']['schedule']),
            'second_missed': beat.ScheduleEntry(
                'second_missed', 'second_missed',
                last_run_at=now_func() - timedelta(minutes=2),
                total_run_count=10,
                app=self.app,
                schedule=app_schedule['second_missed']['schedule']),
            'non_missed': beat.ScheduleEntry(
                'non_missed', 'non_missed',
                last_run_at=now_func() - timedelta(minutes=2),
                total_run_count=10,
                app=self.app,
                schedule=app_schedule['non_missed']['schedule']),
        }

        self.app.conf.beat_schedule = app_schedule

        scheduler = scheduler_class(self.app)

        max_iter_number = 5
        for i in range(max_iter_number):
            delay = scheduler.tick()
            if delay > 0:
                break
        assert {'first_missed', 'second_missed'} == {
            item['task'] for item in scheduler.sent}
        # ensure next call on the beginning of next min
        assert abs(60 - cur_seconds - delay) < 1

    def test_skip_records_persisted_and_reloaded(self):
        shelv = MockShelve()

        class MockPersistentScheduler(beat.PersistentScheduler):
            persistence = Bunch(open=lambda *a, **kw: shelv)

            def __init__(self, *args, **kwargs):
                self.sent = []
                super().__init__(*args, **kwargs)

            def send_task(self, task=None, args=None, kwargs=None,
                          **options):
                self.sent.append(task)
                return self.app.AsyncResult(uuid())

        now = datetime.now(timezone.utc)
        sched = mocked_schedule(True, 300, nowfun=lambda: now)
        scheduler = MockPersistentScheduler(app=self.app,
                                            schedule_filename='schedule')
        entry = scheduler.add(name='late', task='tasks.add', schedule=sched,
                              misfire_grace_time=60)
        entry.next_run_at = now - timedelta(seconds=3600)

        assert scheduler.tick() == 0
        assert not scheduler.sent
        assert len(shelv['skipped_runs']) == 1

        # a new scheduler over the same database sees the records.
        scheduler2 = MockPersistentScheduler(app=self.app,
                                             schedule_filename='schedule')
        skipped = scheduler2.skipped_runs()
        assert len(skipped) == 1
        assert skipped[0].entry == 'late'
        assert skipped[0].reason == beat.SKIP_REASON_MISFIRE


class test_Service:

    def get_service(self):
        Scheduler, mock_shelve = create_persistent_scheduler()
        return beat.Service(
            app=self.app, scheduler_cls=Scheduler), mock_shelve

    def test_pickleable(self):
        s = beat.Service(app=self.app, scheduler_cls=Mock)
        assert loads(dumps(s))

    def test_start(self):
        s, sh = self.get_service()
        schedule = s.scheduler.schedule
        assert isinstance(schedule, dict)
        assert isinstance(s.scheduler, beat.Scheduler)
        scheduled = list(schedule.keys())
        for task_name in sh['entries'].keys():
            assert task_name in scheduled

        s.sync()
        assert sh.closed
        assert sh.synced
        assert s._is_stopped.is_set()
        s.sync()
        s.stop(wait=False)
        assert s._is_shutdown.is_set()
        s.stop(wait=True)
        assert s._is_shutdown.is_set()

        p = s.scheduler._store
        s.scheduler._store = None
        try:
            s.scheduler.sync()
        finally:
            s.scheduler._store = p

    def test_start_embedded_process(self):
        s, sh = self.get_service()
        s._is_shutdown.set()
        s.start(embedded_process=True)

    def test_start_thread(self):
        s, sh = self.get_service()
        s._is_shutdown.set()
        s.start(embedded_process=False)

    def test_start_tick_raises_exit_error(self):
        s, sh = self.get_service()
        s.scheduler.tick_raises_exit = True
        s.start()
        assert s._is_shutdown.is_set()

    def test_start_manages_one_tick_before_shutdown(self):
        s, sh = self.get_service()
        s.scheduler.shutdown_service = s
        s.start()
        assert s._is_shutdown.is_set()


class test_EmbeddedService:

    def xxx_start_stop_process(self):
        pytest.importorskip('_multiprocessing')
        from billiard.process import Process

        s = beat.EmbeddedService(self.app)
        assert isinstance(s, Process)
        assert isinstance(s.service, beat.Service)
        s.service = MockService()

        class _Popen:
            terminated = False

            def terminate(self):
                self.terminated = True

        with patch('celery.platforms.close_open_fds'):
            s.run()
        assert s.service.started

        s._popen = _Popen()
        s.stop()
        assert s.service.stopped
        assert s._popen.terminated

    def test_start_stop_threaded(self):
        s = beat.EmbeddedService(self.app, thread=True)
        from threading import Thread
        assert isinstance(s, Thread)
        assert isinstance(s.service, beat.Service)
        s.service = MockService()

        s.run()
        assert s.service.started

        s.stop()
        assert s.service.stopped


class test_schedule:

    def test_maybe_make_aware(self):
        x = schedule(10, app=self.app)
        x.utc_enabled = True
        d = x.maybe_make_aware(datetime.now(timezone.utc))
        assert d.tzinfo
        x.utc_enabled = False
        d2 = x.maybe_make_aware(datetime.now(timezone.utc))
        assert d2.tzinfo

    def test_to_local(self):
        x = schedule(10, app=self.app)
        x.utc_enabled = True
        d = x.to_local(datetime.now())
        assert d.tzinfo is None
        x.utc_enabled = False
        d = x.to_local(datetime.now(timezone.utc))
        assert d.tzinfo
