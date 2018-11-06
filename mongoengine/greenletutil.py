try:
    import greenlet
except ImportError:
    greenlet = None
import logging
import random
import time
import ujson
from py_enum import PyEnumMixin


def wrap_future(f):
    '''Wrap a Tornado Future with greenlets for the intermediate callbacks
    '''
    # if the future is already done, lets switch back immediately
    # this is to allow for blindly wrapping the future return
    if f.done():
        return f.result()
    current_greenlet = greenlet.getcurrent()
    # add callback once the future is complete
    f.add_done_callback(current_greenlet.switch)
    # wait
    f = current_greenlet.parent.switch()
    # this will return the result or raise the exception
    return f.result()


class GreenletUtil(object):
    @classmethod
    def is_async(cls):
        if not greenlet:
            return False

        current = greenlet.getcurrent()

        if isinstance(current, CLGreenlet) and current.parent:
            return current.async_allowed()

        if not current.parent:
            return False

        return True


base_class = greenlet.greenlet if greenlet else object

traced_slow_queries_logger = logging.getLogger('sweeper.prod.slow_fe_queries')
trace_logger = logging.getLogger('sweeper.prod.fe_trace')


class GreenletDeadException(Exception):
    pass


class CLGreenlet(base_class):
    _ALLOW_ASYNC = True

    def __init__(self, *args, **kwargs):
        if base_class != greenlet.greenlet:
            raise TypeError('cannot use CLGreenlet without greenlet module!')

        super(CLGreenlet, self).__init__(*args, **kwargs)

    def switch(self, *args, **kwargs):
        # https://greenlet.readthedocs.io/en/latest/
        #    "Note that any attempt to switch to a dead greenlet
        #     actually goes to the dead greenlet's parent,
        #     or its parent's parent, and so on. (The final
        #     parent is the "main" greenlet, which is never dead.)"
        #
        # This can cause issues where we context switch into the wrong
        # greenlet, so it gets a return value it's not expecting and
        # borks. To prevent this from happening, raise an exception
        # if we're switching into a greenlet that is dead.
        if self.dead:
            raise GreenletDeadException("Can't switch into dead greenlet")

        return super(CLGreenlet, self).switch(*args, **kwargs)

    # Propagate upwards to let a subclass determine this
    def is_deadlined(self):
        if isinstance(self.parent, CLGreenlet):
            return self.parent.is_deadlined()
        return False

    # XXX: not sure why we loop here. It risks an infinite loop if an
    # exception is raised prior to switching. Context should never return
    # here *unless* that happens, so why do it at all?
    def do_deadline(self):
        # make sure this greenlet is permanently dead
        while True:
            try:
                self.parent.switch()
            except Exception as e:
                if isinstance(e, GreenletDeadException):
                    break

    def add_trace(self, url, kwargs, timeslice):
        if isinstance(self.parent, CLGreenlet):
            self.parent.add_trace(url, kwargs, timeslice)

    def add_grpc_trace(self, service, func, comment, timeslice):
        if isinstance(self.parent, CLGreenlet):
            self.parent.add_grpc_trace(service, func, comment, timeslice)

    def add_mc_trace(self, service, func, comment, timeslice):
        if isinstance(self.parent, CLGreenlet):
            self.parent.add_mc_trace(service, func, comment, timeslice)

    def add_mongo_start(self, comment, start_time):
        if isinstance(self.parent, CLGreenlet):
            self.parent.add_mongo_start(comment, start_time)

    def add_mongo_end(self, comment, end_time, is_scatter_gather):
        if isinstance(self.parent, CLGreenlet):
            self.parent.add_mongo_end(comment, end_time, is_scatter_gather)

    def print_traces(self):
        if isinstance(self.parent, CLGreenlet):
            self.parent.print_trace()

    def log_traced_slow_queries(self):
        if isinstance(self.parent, CLGreenlet):
            self.parent.log_traced_slow_queries()

    def async_allowed(self):
        if isinstance(self.parent, CLGreenlet):
            return self.parent.async_allowed()
        return self._ALLOW_ASYNC


class NonAsyncMixin(object):
    _ALLOW_ASYNC = False


class BaseTrace(object):

    def __init__(self):
        self.timeslice = None

    def to_str(self, origin):
        old_timeslice = self.timeslice
        self.timeslice = self.relative_timeslice(origin)
        ret = self.__str__()
        self.timeslice = old_timeslice
        return ret

    @property
    def start(self):
        if self.timeslice:
            return self.timeslice[0]
        return None

    @property
    def end(self):
        if self.timeslice:
            return self.timeslice[1]
        return None

    @property
    def time(self):
        if self.timeslice:
            return self.timeslice[1] - self.timeslice[0]
        return None

    def relative_timeslice(self, origin):
        if self.timeslice:
            return (self.timeslice[0] - origin,
                    self.timeslice[1] - origin)
        return None


class TraceType(PyEnumMixin):
    MONGO = 1
    EASY_REQUEST = 2
    PYTHON = 3
    GRPC = 4
    MC = 5


class MongoQueryTrace(BaseTrace):

    def __init__(self, comment, timeslice, is_scatter_gather):
        super(MongoQueryTrace, self).__init__()

        self.comment = comment
        self.timeslice = timeslice
        self.is_scatter_gather = is_scatter_gather

    def __str__(self):
        return "MongoQueryTrace %s: %s, is_scatter_gather=%s" % (
            str(self.timeslice), self.comment, str(self.is_scatter_gather))

    def to_dict(self, origin=0):
        return {
            'comment': self.comment,
            'timeslice': self.relative_timeslice(origin),
            'is_scatter_gather': self.is_scatter_gather,
            'type': TraceType.MONGO,
        }


class EasyRequestTrace(BaseTrace):

    def __init__(self, url, kwargs, timeslice):
        super(EasyRequestTrace, self).__init__()

        self.url = url
        self.kwargs = kwargs
        self.kwargs.pop('callback', None)
        self.kwargs.pop('prepare_curl_callback', None)
        self.timeslice = timeslice

    def __str__(self):
        return "EasyRequestTrace %s: %s, kwargs=%s" % (
            str(self.timeslice), self.url, str(self.kwargs))

    def to_dict(self, origin=0):
        return {
            'url': self.url,
            'timeslice': self.relative_timeslice(origin),
            'kwargs': {},
            'type': TraceType.EASY_REQUEST,
        }


class GRPCTrace(BaseTrace):

    def __init__(self, service, func, comment, timeslice):
        super(GRPCTrace, self).__init__()

        self.service = service
        self.func = func
        self.timeslice = timeslice
        self.comment = comment

    def __str__(self):
        return "GRPCTrace %s: %s.%s, %s" % (
            str(self.timeslice), self.service, self.func,
            self.comment)

    def to_dict(self, origin=0):
        return {
            'service': self.service,
            'timeslice': self.relative_timeslice(origin),
            'func': self.func,
            'type': TraceType.GRPC,
            'comment': self.comment,
        }


class MCTrace(BaseTrace):

    def __init__(self, service, func, comment, timeslice):
        super(MCTrace, self).__init__()

        self.service = service
        self.func = func
        self.timeslice = timeslice
        self.comment = comment

    def __str__(self):
        return "MCTrace %s: %s.%s, %s" % (
            str(self.timeslice), self.service, self.func,
            self.comment)

    def to_dict(self, origin=0):
        return {
            'service': self.service,
            'timeslice': self.relative_timeslice(origin),
            'func': self.func,
            'type': TraceType.MC,
            'comment': self.comment,
        }


class PythonSyncBlock(BaseTrace):

    def __init__(self, timeslice, pre_trace=None, post_trace=None):
        super(PythonSyncBlock, self).__init__()

        self.pre_trace = pre_trace
        self.post_trace = post_trace
        self.timeslice = timeslice

    def __str__(self):
        return "PythonSyncBlock %s" % (str(self.timeslice))

    def to_dict(self, origin=0):
        return {
            'timeslice': self.relative_timeslice(origin),
            'type': TraceType.PYTHON,
        }


class CLTracingGreenlet(CLGreenlet):
    SLOW_QUERY_THRESHOLD = 0  # ms
    SLOW_SCATTER_GATHER_QUERY_THRESHOLD = 0  # ms

    def __init__(self, *args, **kwargs):
        self._easy_request_traces = []
        self._mongo_traces = {}
        self._nonpipelined_mongo_traces = []
        self._grpc_traces = []
        self._mc_traces = []
        super(CLTracingGreenlet, self).__init__(*args, **kwargs)

    def async_allowed(self):
        return self._ALLOW_ASYNC

    @property
    def easy_request_traces(self):
        return self._easy_request_traces

    @property
    def grpc_traces(self):
        return self._grpc_traces

    @property
    def mc_traces(self):
        return self._mc_traces

    def add_grpc_trace(self, service, func, comment, timeslice):
        self._grpc_traces.append(
            GRPCTrace(service, func, comment, timeslice))

    def add_mc_trace(self, service, func, comment, timeslice):
        self._mc_traces.append(
            MCTrace(service, func, comment, timeslice))

    @property
    def mongo_traces(self):
        traces = []
        for comment, trace_info in self._mongo_traces.iteritems():
            start, end, is_scatter_gather = trace_info
            if end is not None:
                traces.append(MongoQueryTrace(
                    comment, (start, end), is_scatter_gather))
            else:
                logging.debug('Could not find end time for query %s',
                              comment)
        return traces + self._nonpipelined_mongo_traces

    def sort_traces(self, traces):
        return sorted(traces, key=lambda t: t.time)

    def print_traces_sorted(self):
        logging.info("\n".join(
            ["[%0.2fms] %s" % ((t.time) * 1000, str(t))
                for t in self.full_trace_sorted()]))

    def print_traces(self):
        ft = self.full_trace()
        if len(ft) > 0:
            start_time = ft[0].start
            logging.info("\n".join(
                ["[%0.2fms] %s" % ((t.time) * 1000, t.to_str(start_time))
                    for t in ft]))
            non_python_time = \
                sum([t.time if not
                     isinstance(t, PythonSyncBlock) else 0 for t in ft])
            logging.info("Total time in async operations: [%f ms]",
                         non_python_time * 1000)
            logging.info("Total time in sync/python/waiting operations: [%f ms]",
                         self.get_python_time() * 1000)

    def add_trace(self, url, kwargs, timeslice):
        self._easy_request_traces.append(
            EasyRequestTrace(url, kwargs, timeslice))

    def add_mongo_start(self, comment, start_time):
        if comment not in self._mongo_traces:
            self._mongo_traces[comment] = (start_time, None, False)

    def add_mongo_end(self, comment, end_time, is_scatter_gather):
        if comment in self._mongo_traces:
            start, end, isg = self._mongo_traces[comment]
            self._mongo_traces[comment] = \
                (start, max(end, end_time) if
                    end is not None else end_time,
                    isg or is_scatter_gather)
            if 'pipeline call' not in comment:
                start, end, isg = self._mongo_traces[comment]
                self._nonpipelined_mongo_traces.append(
                    MongoQueryTrace(comment, (start, end), isg))
                del self._mongo_traces[comment]

    def full_trace_sorted(self):
        return self.sort_traces(self.all_traces)

    def full_trace(self):
        _sorted = sorted(self.all_traces, key=lambda t: t.start)
        if len(_sorted) <= 0:
            return []

        glob = (_sorted[0].start, _sorted[0].end)
        sync_blocks = []
        for t in _sorted[1:]:
            if t.start < glob[1]:
                glob = (glob[0], t.start)
            else:
                sync_blocks.append(PythonSyncBlock((glob[1], t.start)))
                glob = t.timeslice

        if len(_sorted[1:]) >= 1:
            sync_blocks.append(PythonSyncBlock((t.end + 1e-9, time.time())))

        results = sorted(_sorted + sync_blocks, key=lambda t: t.start)
        return results

    def full_mongo_trace(self):
        _sorted = sorted(self.mongo_traces, key=lambda t: t.start)
        if len(_sorted) <= 0:
            return []

        glob = (_sorted[0].start, _sorted[0].end)
        sync_blocks = []
        for t in _sorted[1:]:
            if t.start < glob[1]:
                glob = (glob[0], t.start)
            else:
                sync_blocks.append(PythonSyncBlock((glob[1], t.start)))
                glob = t.timeslice

        if len(_sorted[1:]) >= 1:
            sync_blocks.append(PythonSyncBlock((t.end + 1e-9, time.time())))
        return sorted(_sorted + sync_blocks, key=lambda t: t.start)

    def log_traced_slow_queries(self):
        for trace in self.full_mongo_trace():
            if not isinstance(trace, MongoQueryTrace):
                continue

            query_time = (trace.time) * 1000

            query_ms_threshold = self.SLOW_SCATTER_GATHER_QUERY_THRESHOLD \
                if trace.is_scatter_gather else self.SLOW_QUERY_THRESHOLD

            if query_time > query_ms_threshold and \
                    random.random() <= 0.01:
                traced_slow_queries_logger.info(
                    {
                        'comment': trace.comment,
                        'query_time': query_time,
                        '_uri': self._request.uri,
                        '_app': self._handler.application.sysmon_name(),
                        'is_scatter_gather': trace.is_scatter_gather,
                        'monitor_key': self._handler.monitor_key if self._handler
                        and self._handler.monitor_key else '',
                    }
                )

    def log_full_trace(self):
        if self._handler and self._handler.monitor_key:
            if random.random() <= 0.01:
                full_trace = self.full_trace()

                if len(full_trace) <= 0:
                    return

                origin = full_trace[0].start

                dict_traces = [trace.to_dict(origin=origin)
                               for trace in full_trace]

                app = ''
                try:
                    app = self._handler.application.sysmon_name()
                except Exception:
                    pass

                trace_logger.info({
                    'monitor_key': self._handler.monitor_key,
                    'traces': ujson.dumps(dict_traces),
                    'application': app
                })

    @property
    def all_traces(self):
        return self.easy_request_traces + self.grpc_traces + self.mc_traces

    def get_python_time(self):
        gr = greenlet.getcurrent()
        while gr:
            if isinstance(gr, CLTracingGreenlet):
                python_time = \
                    sum([t.timeslice[1] - t.timeslice[0]
                         for t in gr.full_trace() if
                         isinstance(t, PythonSyncBlock)])
                return python_time
            gr = gr.parent
        return None


class CLBackendTracingGreenlet(CLTracingGreenlet):

    def __init__(self, *args, **kwargs):
        self._app = None

        super(CLBackendTracingGreenlet, self).__init__(
            *args, **kwargs)

    @property
    def app(self):
        return self._app

    @app.setter
    def app(self, app):
        self._app = app

    def add_mongo_end(self, *args, **kwargs):
        super(CLBackendTracingGreenlet, self).add_mongo_end(
            *args, **kwargs)

        self.log_traced_slow_queries(self._nonpipelined_mongo_traces)

        self._nonpipelined_mongo_traces = []

    def log_traced_slow_queries(self, traces):
        for trace in traces:
            if isinstance(trace, (PythonSyncBlock, EasyRequestTrace)):
                continue

            query_time = (trace.time) * 1000

            query_ms_threshold = self.SLOW_SCATTER_GATHER_QUERY_THRESHOLD \
                if trace.is_scatter_gather else self.SLOW_QUERY_THRESHOLD

            if query_time > query_ms_threshold and \
                    random.random() <= 0.01:
                traced_slow_queries_logger.info(
                    {
                        'comment': trace.comment,
                        'query_time': query_time,
                        '_app': self.app,
                        'is_scatter_gather': trace.is_scatter_gather
                    }
                )


class NonAsyncCLBackendTracingGreenlet(NonAsyncMixin, CLBackendTracingGreenlet):
    _ALLOW_ASYNC = False
