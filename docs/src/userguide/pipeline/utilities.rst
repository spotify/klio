Utilities
=========

Klio offers decorators and other utilities that help Klio-ify transforms of a pipeline.


.. _serialization-klio-message:

De/serialization of Klio Messages
---------------------------------


.. _handle-klio:

``@handle_klio``
^^^^^^^^^^^^^^^^

:func:`@handle_klio <klio.transforms.decorators.handle_klio>` generates a :class:`KlioContext
<klio.transforms.core.KlioContext>` instance as well as handles the de/serialization of the
incoming PCollection as a ``KlioMessage``.

.. _klio-context-decorators:

Under the hood
^^^^^^^^^^^^^^

Decorating a class method with :func:`@handle_klio <klio.transforms.decorators.handle_klio>` will
first set the :class:`KlioContext <klio.transforms.core.KlioContext>` instance on the class
instance as ``self._klio``. Decorating a function will provide the :class:`KlioContext
<klio.transforms.core.KlioContext>` instance as the first argument of the function. For both
methods and functions, the decorator handles de/serialization of a ``KlioMessage`` to/from
protobuf.


.. code-block:: python

    from klio.transforms import decorators

    # Decorating a method on a DoFn sets a KlioContext
    # instance on self._klio
    class MyKlioDoFn(beam.DoFn):
        @decorators.handle_klio
        def process(self, item):
            self._klio.logger.info(f"Received element {item.element}")
            yield item


    # Decorating a method on a composite transform sets a
    # KlioContext instance on self._klio
    class MyKlioComposite(beam.PTransform):
        @decorators.handle_klio
        def expand(self, pcoll):
            kms_config = self._klio.config.job_config.kms_config
            return pcoll | MyKMSTransform(**kms_config)


    # Decorating a function passes a KlioContext instance as
    # the first argument
    @decorators.handle_klio
    def my_map_func(ctx, item):
        ctx.logger.info(f"Received {item.element} with {item.payload}")


.. _setting-klio-context:

``@serialize_klio_message``
^^^^^^^^^^^^^^^^^^^^^^^^^^^

:func:`@serialize_klio_message <klio.transforms.decorators.serialize_klio_message>` can be used
for more fine-grained control of de/serialization of incoming PCollections of KlioMessages. This
decorator expects access to a :class:`KlioContext <klio.transforms.core.KlioContext>` object (see
:ref:`@inject_klio_context <inject-klio-context>` or :ref:`@set_klio_context <set-klio-context>`).


.. code-block:: python

    from klio.transforms import decorators

    class MyKlioDoFn(beam.DoFn):
        @decorators.set_klio_context
        def setup(self):
            data_config = self._klio.config.job_config.data
            self.input_directory = data_config.inputs[0].location
            self.output_directory = data_config.outputs[0].location

        @decorators.serialize_klio_message
        def process(self, item):
            entity_id = item.element
            output_file_path = f"{self.output_directory}/{entity_id}.mp3"


.. tip::

    Functions and methods decorated with :func:`@serialize_klio_message
    <klio.transforms.decorators.serialize_klio_message>` will handle the same de/serialize
    functionality as ``@handle_klio`` but will not set or inject :class:`KlioContext
    <klio.transforms.core.KlioContext>`. This decorator expects access to a ``KlioContext``
    object. If granular control is not needed, then see :ref:`@handle_klio <handle-klio>` which
    handles both context and de/serialization.


.. _accessing-klio-context:

Accessing Klio Context
----------------------

.. _set-klio-context:

``@set_klio_context``
^^^^^^^^^^^^^^^^^^^^^

:func:`@set_klio_context <klio.transforms.decorators.set_klio_context>` is used on a class method
to set a :class:`KlioContext <klio.transforms.core.KlioContext>` instance on the class as the
instance attribute ``self._klio``.

.. code-block:: python

    from klio.transforms import decorators

    class HelloKlioDoFn(beam.DoFn):
        @decorators.set_klio_context
        def setup(self):
            data_config = self._klio.config.job_config.data
            self.input_config = data_config.inputs
            self.output_config = data_config.outputs


.. tip::

    Methods decorated with :func:`@set_klio_context <klio.transforms.decorators.set_klio_context>`
    will not handle ``KlioMessage`` de/serialize functionality.

    ``@set_klio_context`` should be used on a class method. If :class:`KlioContext
    <klio.transforms.core.KlioContext>` is needed on a function, see :ref:`@inject_klio_context
    <inject-klio-context>`. If KlioMessage de/serialization functionality is needed, see
    :ref:`@handle_klio <handle-klio>`.


.. _inject-klio-context:

``@inject_klio_context``
^^^^^^^^^^^^^^^^^^^^^^^^

:func:`@inject_klio_context <klio.transforms.decorators.inject_klio_context>` provides a
:class:`KlioContext <klio.transforms.core.KlioContext>` instance as the first argument to a
function.

.. code-block:: python

    from klio.transforms import decorators

    @decorators.inject_klio_context
    def my_map_func(ctx, element):
        ctx.logger.info(f"Received {element}")


    class HelloKlioDoFn(beam.DoFn):
        @decorators.inject_klio_context
        def process(self, ctx, element):
            ctx.logger.info(f"Received {element}")

.. tip::

    :func:`@inject_klio_context <klio.transforms.decorators.inject_klio_context>` should be used
    on a function. If :class:`KlioContext <klio.transforms.core.KlioContext>` is needed on a
    method, see :ref:`@set_klio_context <set-klio-context>`. If KlioMessage de/serialization
    functionality is needed, see :ref:`@handle_klio <handle-klio>`.

Handling Transient Errors
-------------------------

.. _timeout:

``@timeout``
^^^^^^^^^^^^

:func:`@timeout <klio.transforms.decorators.timeout>` will run the decorated method or function
with a timeout in a separate Python process. On timeout, the method or function will raise an
exception of the provided type or default to raising a ``KlioTimeoutError``.

.. caution::

    If ``@timeout`` is being used with :ref:`@retry <retries>`, **order is important** depending
    on the desired effect.

    If ``@timeout`` is applied to a function before ``@retry``, then retries will apply first,
    meaning the configured timeout will cancel the function even if the retries have not yet been
    exhausted. In this case, be careful with the ``delay`` argument for the ``@retry`` decorator:
    the set timeout is inclusive of a retry's delay.

    Conversely, if ``@retry`` is applied to a function before ``@timeout``, retries will continue
    until exhausted even if a function has timed out.

.. code-block:: python

    from klio.transforms import decorators

    class MyDoFn(beam.DoFn):
        @decorators.timeout(seconds=5, exception=MyTimeoutException)
        def process(self, item):
            self._klio.logger.info(
                f"Received {item.element} with {item.payload}"
            )


    @decorators.timeout(
        seconds=5,
        exception=MyTimeoutException,
        exception_message="I got a timeout!"
    )
    def my_nonklio_map_func(item):
        print(f"Received {item}!")


.. caution::

    If in use with another Klio decorator, the :func:`@timeout
    <klio.transforms.decorators.timeout>` decorator should be applied to a method or function
    **after** the other Klio decorator.

    .. code-block:: python

        from klio.transforms import decorators

        @decorators.handle_klio
        @decorators.timeout(seconds=5)
        def my_map_func(ctx, item):
            ctx.logger.info(f"Received {item.element} with {item.payload}")


        class MyDoFn(beam.DoFn):
            @decorators.handle_klio
            @decorators.timeout(seconds=5, exception=MyTimeoutException)
            def process(self, item):
                self._klio.logger.info(
                    f"Received {item.element} with {item.payload}"
                )


.. _retries:

``@retry``
^^^^^^^^^^

:func:`@retry <klio.transforms.decorators.retry>` will retry the decorated method or function on
failure. When retries are exhausted, ``KlioRetriesExhausted`` exception will be raised. Unless
otherwise configured, a method or function decorated by ``@retry`` will be retried infinitely.

.. caution::

    If ``@retry`` is being used with :ref:`@timeout <timeout>`, **order is important** depending
    on the desired effect.

    If ``@timeout`` is applied to a function before ``@retry``, then retries will apply first,
    meaning the configured timeout will cancel the function even if the retries have not yet been
    exhausted. In this case, be careful with the ``delay`` argument for the ``@retry`` decorator:
    the set timeout is inclusive of a retry's delay.

    Conversely, if ``@retry``  is applied to a function before ``@timeout``, retries will continue
    until exhausted even if a function has timed out.


.. code-block:: python

    from klio.transforms import decorators

    @decorators.handle_klio
    @decorators.retry()  # infinite retries, same as tries=-1
    def my_map_func(ctx, item):
        ctx.logger.info(f"Received {item.element} with {item.payload}")
        ...


    class MyDoFn(beam.DoFn):
        @decorators.handle_klio
        @decorators.retry(tries=3, exception=MyExceptionToCatch)
        def process(self, item):
            self._klio.logger.info(f"Received {item.element} with {item.payload}")
            ...


    # all available keyword arguments
    @decorators.handle_klio
    @decorators.retry(
        tries=3,
        delay=2.5,  # seconds
        exception=MyExceptionToCatch,
        raise_exception=MyExceptionToRaise,
        exception_message="All retries have been exhausted!"
    )
    def my_other_map_function(item):
        print(f"Received {item}!")
        ...


.. caution::

    If in use with another Klio decorator, the :func:`@retry <klio.transforms.decorators.retry>`
    decorator should be applied to a method or function **after** the other Klio decorator.

    .. code-block:: python

        from klio.transforms import decorators

        @decorators.handle_klio
        @decorators.retry(tries=5)
        def my_map_func(ctx, item):
            ctx.logger.info(f"Received {item.element} with {item.payload}")
            ...


        class MyDoFn(beam.DoFn):
            @decorators.handle_klio
            @decorators.retries(tries=5, exception=MyExceptionToCatch)
            def process(self, item):
                self._klio.logger.info(f"Received {item.element}")
                ...


.. _profile:

``@profile``
^^^^^^^^^^^^

:func:`@profile <klio.transforms.decorators.profile>` will mark the decorated
method or function for profiling.  This is used in conjunction with the ``klio
job profile`` commands to selectively profile parts of your pipeline.  This
decorator can be added to any function or method, but when using with other
Klio decorators such as ``@handle_klio`` it **must** be the last decorator
applied.

When running/testing a job normally and not profiling, this decorator has no
effect.

.. code-block:: python

    @handle_klio
    @profile
    def my_map_func(ctx, item):
        ctx.logger.info(f"Received {item.element} with {item.payload}")

    class MyDoFn(beam.DoFn):
        @handle_klio
        @profile
        def process(self, item):
            self._klio.logger.info(
                f"Received {item.element} with {item.payload}"
            )

    @profile
    def my_nonklio_map_func(item):
        print(f"Received {item}!")


.. _klio-concurrency-mgmt:

Concurrency Management
----------------------

Currently, Apache Beam does not have a mechanism to limit the number of threads that a pipeline uses to execute its transforms.
Klio provides a way for users to limit the number of threads used for a specific transform or section of code, with defaults set to the number of CPUs of the worker machine.
See :doc:`../../../keps/kep-002` for more background information.

There are two mechanisms to manage threads in a Klio pipeline:

1. the :func:`@handle_klio <klio.transforms.decorators.handle_klio>` decorator;
2. and a context manager.

Both of these approaches can be seen in the following examples:

.. code-block:: python

    # Limit threads via the @handle_klio decorator
    from klio.transforms import decorators
    @decorators.handle_klio(max_thread_count=2)
    def my_heavy_weight_transform(ctx, item):
        ...

    # If `max_thread_count` isn't provided, default of # of CPUs will be used
    @decorators.handle_klio
    def my_heavy_weight_ransform(ctx, item):
        ...

    # Limit threads with a context manager
    from klio.utils import ThreadLimiter
    thread_limiter = ThreadLimiter(max_thread_count=2)
    with thread_limiter:
        ...


Refer to the :class:`klio.utils.ThreadLimiter` definition for supported arguments.

Each transform that uses the ``@handle_klio`` decorator or the thread limiter context manager will have their own "pseudo-pool" of threads they're allowed to use managed by a :class:`BoundedSemaphore <threading.BoundedSemaphore>`.
When using the decorator, the number of threads given to a transform equates to the number of elements that the transform can process at any given time.
Therefore, if all threads in an allotted pool are in use, then the transform will be blocked from processing a new element until an in-process element is complete or errors out.

While limiting the threads for one transform does not `directly` limit the threads of another transform, if a transform is limited to fewer threads precedes a transform with a higher number of threads – or no limitation – then the latter transform may be indirectly affected and not able to use all of its available threads.

.. note::

    Klio's thread management does not affect jobs using the direct runner `unless` the `option <https://beam.apache.org/documentation/runners/direct/#parallel-execution>`_ ``direct_running_mode='multi_threading'`` is used in the job's ``pipeline_options``.
    By default, pipelines run using the direct runner are single-threaded.
