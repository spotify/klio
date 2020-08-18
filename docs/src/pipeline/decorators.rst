Utilities
=============

Klio offers decorators that help:
 * :ref:`De/serialization of Klio Messages <serialization-klio-message>`
 * :ref:`Inject klio context on methods and functions <accessing-klio-context>`
 * :ref:`Handle timeouts <timeout>`


.. _serialization-klio-message:

De/serialization of klio messages:
------------------------------------


.. _handle-klio:

**@handle_klio**
^^^^^^^^^^^^^^^^^^

``@handle_klio`` generates a ``KlioContext`` instance as well as handles the de/serialization of the
incoming PCollection as a Klio Message.

Under the hood:
""""""""""""""""""

Decorating a class method with ``@handle_klio`` will first set the ``KlioContext`` instance on the
class on the property ``_klio``. Decorating a function will provide the ``KlioContext`` instance as
the first argument of the function. The decorator then implements de/serialization logic assuming a
raw, serialized incoming Klio message to parse then yield the element.


.. code-block:: python

    from klio.transforms import decorators

    # Decorating a method on a DoFn sets KlioContext instance on self._klio
    class MyKlioDoFn(beam.DoFn):
        @decorators.handle_klio
        def process(self, item):
            self._klio.logger.info("Received element {}".format(item.element))
            yield item


    # Decorating a method on a composite transform sets KlioContext instance on self._klio
    class MyKlioComposite(beam.PTransform):
        @decorators.handle_klio
        def expand(self, pcoll):
            kms_config = self._klio.config.job_config.kms_config
            return pcoll | MyKMSTransform(**kms_config)

    # Decorating a function passes KlioContext instance as first argument
    @decorators.handle_klio
    def my_map_func(ctx, item):
        ctx.logger.info(f"Received {item.element} with {item.payload}")


.. _setting-klio-context:

**@serialize_klio_message**
^^^^^^^^^^^^^^^^^^^^^^^^^^^^

``@serialize_klio_message`` can be used for more fine-grained control of de/serialization of
incoming PCollections as a KlioMessage. This decorator needs access to a ``KlioContext`` object (see
:ref:`@inject_klio_context <inject-klio-context>` or :ref:`@set_klio_context <set-klio-context>`).


.. code-block:: python

    from klio.transforms import decorators

    class MyKlioDoFn(beam.DoFn):
        @decorators.set_klio_context
        def setup(self):
            self.input_directory = self._klio.config.job_config.data_inputs[0].location
            self.output_directory = self._klio.config.job_config.data_outputs[0].location

        @decorators.serialize_klio_message
        def process(self, item):
            entity_id = item.element
            output_file_path = "{}/{}.mp3".format(self.output_directory, entity_id)

.. tip::

    Functions and methods decorated with ``@serialize_klio_message`` will handle the same deserialize
    functionality as ``@handle_klio`` but will exclude actions to set the ``KlioContext`` instance as class
    property or function argument. If granular control is not needed then see :ref:`@handle_klio
    <handle-klio>` which handles both context and de/serialization.


.. _accessing-klio-context:

Accessing Klio Context:
------------------------

.. _set-klio-context:

**@set_klio_context**
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

``@set_klio_context`` is used on a class method to set the ``KlioContext`` instance on the class
on the property ``_klio``.

.. code-block:: python

    from klio.transforms import decorators

    class HelloKlioDoFn(beam.DoFn):
        @decorators.set_klio_context
        def setup(self):
            self.input_config = self._klio.config.job_config.data_inputs
            self.output_config = self._klio.config.job_config.data_outputs


.. tip::

    Functions and methods decorated with ``@set_klio_context`` will handle actions to set the
    ``KlioContext`` instance as class property or function as ``@handle_klio`` but excludes the
    de/serialize functionality.

    ``@set_klio_context`` should be used on a class method. If ``KlioContext`` is needed on a function, see
    :ref:`@inject_klio_context <inject-klio-context>`. If KlioMessage de/serialization functionality is needed, see :ref:`@handle_klio <handle-klio>`.


.. _inject-klio-context:

**@inject_klio_context**
^^^^^^^^^^^^^^^^^^^^^^^^^^

``@inject_klio_context`` provides the ``KlioContext`` instance as the first argument to a function.

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

    ``@inject_klio_context`` should be used on a function. If ``KlioContext`` is needed on a method, see :ref:`@set_klio_context <set-klio-context>`
    If KlioMessage de/serialization functionality is needed, see :ref:`@handle_klio <handle-klio>`.


Timeouts:
------------------------

.. _timeout:

**@timeout**
^^^^^^^^^^^^^^^^^^^^^

``@timeout`` will run the decorated method or function with a timeout in a separate process. On
timeout, the method or function will raise an exception of the provided type or default to raising a
``KlioTimeoutError``.

.. code-block:: python

    from klio.transforms import decorators

    class MyDoFn(beam.DoFn):
        @decorators.timeout(seconds=5, exception=MyTimeoutException)
        def process(self, item):
            self._klio.logger.info(
                f"Received {item.element} with {item.payload}"
            )


    @timeout(
        seconds=5,
        exception=MyTimeoutException,
        exception_message="I got a timeout!"
    )
    def my_nonklio_map_func(item):
        print(f"Received {item}!")


If in use with another Klio decorator, the `@timeout` decorator should be applied to a method or
function **after** the other Klio decorator.

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
