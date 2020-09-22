from timeit import default_timer as timer

class Timer(object):
    """Timer class supporting multiple independent labelled timers.

    The timer is based on the relative time returned by
    :func:`timeit.default_timer`.
    """

    def __init__(self, labels=None, dfltlbl='main', alllbl='all'):
        """Initialize timer object.

        Args:
            labels (string or list, optional (default None)):
                Specify the label(s) of the timer(s) to be initialised to zero.
            dfltlbl (string, optional (default 'main')):
                Set the default timer label to be used when methods are
                called without specifying a label
            alllbl (string, optional (default 'all')):
                Set the label string that will be used to denote all timer labels
        """

        # Initialise current and accumulated time dictionaries
        self.t0 = {}
        self.td = {}
        # Record default label and string indicating all labels
        self.dfltlbl = dfltlbl
        self.alllbl = alllbl
        # Initialise dictionary entries for labels to be created
        # immediately
        if labels is not None:
            if not isinstance(labels, (list, tuple)):
                labels = [labels,]
            for lbl in labels:
                self.td[lbl] = 0.0
                self.t0[lbl] = None

    def start(self, labels=None):
        """Start specified timer(s).

        Args:
            labels (string or list, optional (default None)):
                Specify the label(s) of the timer(s) to be started. If it is
                ``None``, start the default timer with label specified by the
                ``dfltlbl`` parameter of :meth:`__init__`.
        """

        # Default label is self.dfltlbl
        if labels is None:
            labels = self.dfltlbl
        # If label is not a list or tuple, create a singleton list
        # containing it
        if not isinstance(labels, (list, tuple)):
            labels = [labels,]
        # Iterate over specified label(s)
        t = timer()
        for lbl in labels:
            # On first call to start for a label, set its accumulator to zero
            if lbl not in self.td:
                self.td[lbl] = 0.0
                self.t0[lbl] = None
            # Record the time at which start was called for this lbl if
            # it isn't already running
            if self.t0[lbl] is None:
                self.t0[lbl] = t

    def stop(self, labels=None):
        """Stop specified timer(s).

        Args:
            labels: string or list, optional (default None)
                Specify the label(s) of the timer(s) to be stopped. If it is
                ``None``, stop the default timer with label specified by the
                ``dfltlbl`` parameter of :meth:`__init__`. If it is equal to
                the string specified by the ``alllbl`` parameter of
                :meth:`__init__`, stop all timers.
        """

        # Get current time
        t = timer()
        # Default label is self.dfltlbl
        if labels is None:
            labels = self.dfltlbl
        # All timers are affected if label is equal to self.alllbl,
        # otherwise only the timer(s) specified by label
        if labels == self.alllbl:
            labels = self.t0.keys()
        elif not isinstance(labels, (list, tuple)):
            labels = [labels,]
        # Iterate over specified label(s)
        for lbl in labels:
            if lbl not in self.t0:
                raise KeyError('Unrecognized timer key %s' % lbl)
            # If self.t0[lbl] is None, the corresponding timer is
            # already stopped, so no action is required
            if self.t0[lbl] is not None:
                # Increment time accumulator from the elapsed time
                # since most recent start call
                self.td[lbl] += t - self.t0[lbl]
                # Set start time to None to indicate timer is not running
                self.t0[lbl] = None

    def reset(self, labels=None):
        """Reset specified timer(s).

        Args:
            labels (string or list, optional (default None)):
                Specify the label(s) of the timer(s) to be stopped. If it is
                ``None``, stop the default timer with label specified by the
                ``dfltlbl`` parameter of :meth:`__init__`. If it is equal to
                the string specified by the ``alllbl`` parameter of
                :meth:`__init__`, stop all timers.
        """

        # Get current time
        t = timer()
        # Default label is self.dfltlbl
        if labels is None:
            labels = self.dfltlbl
        # All timers are affected if label is equal to self.alllbl,
        # otherwise only the timer(s) specified by label
        if labels == self.alllbl:
            labels = self.t0.keys()
        elif not isinstance(labels, (list, tuple)):
            labels = [labels,]
        # Iterate over specified label(s)
        for lbl in labels:
            if lbl not in self.t0:
                raise KeyError('Unrecognized timer key %s' % lbl)
            # Set start time to None to indicate timer is not running
            self.t0[lbl] = None
            # Set time accumulator to zero
            self.td[lbl] = 0.0

    def elapsed(self, label=None, total=True):
        """Get elapsed time since timer start.

        Args:
            label (string, optional (default None)):
                Specify the label of the timer for which the elapsed time is
                required.  If it is ``None``, the default timer with label
                specified by the ``dfltlbl`` parameter of :meth:`__init__`
                is selected.
            total (bool, optional (default True)):
                If ``True`` return the total elapsed time since the first
                call of :meth:`start` for the selected timer, otherwise
                return the elapsed time since the most recent call of
                :meth:`start` for which there has not been a corresponding
                call to :meth:`stop`.

        Returns:
            dlt (float):
                Elapsed time
        """

        # Get current time
        t = timer()
        # Default label is self.dfltlbl
        if label is None:
            label = self.dfltlbl
            # Return 0.0 if default timer selected and it is not initialised
            if label not in self.t0:
                return 0.0
        # Raise exception if timer with specified label does not exist
        if label not in self.t0:
            raise KeyError('Unrecognized timer key %s' % label)
        # If total flag is True return sum of accumulated time from
        # previous start/stop calls and current start call, otherwise
        # return just the time since the current start call
        te = 0.0
        if self.t0[label] is not None:
            te = t - self.t0[label]
        if total:
            te += self.td[label]

        return te

    def labels(self):
        """Get a list of timer labels.

        Returns:
            lbl (list):
                List of timer labels
        """

        return self.t0.keys()

    def __str__(self):
        """Return string representation of object.

        The representation consists of a table with the following columns:

          * Timer label
          * Accumulated time from past start/stop calls
          * Time since current start call, or 'Stopped' if timer is not
            currently running
        """

        # Get current time
        t = timer()
        # Length of label field, calculated from max label length
        lfldln = max([len(lbl) for lbl in self.t0] + [len(self.dfltlbl),]) + 2
        # Header string for table of timers
        s = '%-*s  Accum.       Current\n' % (lfldln, 'Label')
        s += '-' * (lfldln + 25) + '\n'
        # Construct table of timer details
        for lbl in sorted(self.t0):
            td = self.td[lbl]
            if self.t0[lbl] is None:
                ts = ' Stopped'
            else:
                ts = ' %.2e s' % (t - self.t0[lbl])
            s += '%-*s  %.2e s  %s\n' % (lfldln, lbl, td, ts)

        return s


class ContextTimer(object):
    """A wrapper class for :class:`Timer` that enables its use as a
    context manager.

    For example, instead of
    .. code-block:: python
        >>> t = Timer()
        >>> t.start()
        >>> do_something()
        >>> t.stop()
        >>> elapsed = t.elapsed()

    one can use
    .. code-block:: python
        >>> t = Timer()
        >>> with ContextTimer(t):
        ...   do_something()
        >>> elapsed = t.elapsed()
    """

    def __init__(self, timer=None, label=None, action='StartStop'):
        """Initialise context manager timer wrapper.

        Args:
            timer (class:`Timer` object, optional (default None)):
                Specify the timer object to be used as a context manager. If
                ``None``, a new class:`Timer` object is constructed.
            label (string, optional (default None)):
                Specify the label of the timer to be used. If it is ``None``,
                start the default timer.
            action (string, optional (default 'StartStop')):
                Specify actions to be taken on context entry and exit. If
                the value is 'StartStop', start the timer on entry and stop
                on exit; if it is 'StopStart', stop the timer on entry and
                start it on exit.
        """

        if action not in ['StartStop', 'StopStart']:
            raise ValueError('Unrecognized action %s' % action)
        if timer is None:
            self.timer = Timer()
        else:
            self.timer = timer
        self.label = label
        self.action = action

    def __enter__(self):
        """Start the timer and return this ContextTimer instance."""

        if self.action == 'StartStop':
            self.timer.start(self.label)
        else:
            self.timer.stop(self.label)
        return self

    def __exit__(self, type, value, traceback):
        """Stop the timer and return True if no exception was raised within
        the 'with' block, otherwise return False.
        """

        if self.action == 'StartStop':
            self.timer.stop(self.label)
        else:
            self.timer.start(self.label)
        if type:
            return False
        else:
            return True

    def elapsed(self, total=True):
        """Return the elapsed time for the timer.

        Args:
            total (bool, optional (default True)):
                If ``True`` return the total elapsed time since the first
                call of :meth:`start` for the selected timer, otherwise
                return the elapsed time since the most recent call of
                :meth:`start` for which there has not been a corresponding
                call to :meth:`stop`.

        Returns:
            dlt (float):
                Elapsed time
        """

        return self.timer.elapsed(self.label, total=total)