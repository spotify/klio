Klio CLI
========

*latest release*: |klio-cli-version| (:doc:`What's new? <changelog>`)

.. todo::

    Write intro documentation to the ``klio-cli`` package.


.. toctree::
   :maxdepth: 1

   api
   changelog


Profiling
=========

.. caution::
   Profiling support is currently limited and may not work for your job!


Currently Klio supports profiling single-argument functions (like those used in
Beam ``Map`` and ``Flatmap`` transforms), ``DoFn`` classes, and partially
supports methods in arbitrary classes.

To profile a function, just add the ``profile`` decorator:

.. code-block:: python

    @decorators.handle_klio
    @decorators.profile
    def my_map_func(ctx, item):
        yield item



Be sure that ``profile`` is the *last* decorator applied to the function.

To add profiling to any class that extends ``DoFn``, add the decorator to the class itself:

.. code-block:: python

    @decorators.profile
    class MyDoFn(beam.DoFn):

        @decorators.handle_klio
        def process(self, item):
            yield item


Lastly, you can add profiling to any class method, however be aware that the class itself will never be instantiated and ``self`` will be ``None``:

.. code-block:: python

    class MyClass(object):

        @decorators.handle_klio
        @decorators.profile
        def process(self, item):
            yield item


How Does Profiling Work
------------------------

When ``klio job profile`` is run, Klio will look for any object with the
``profile`` decorator and add that object to a custom pipline.  Then, it will
take the ids provided on the command-line, duplicate each one a number of
times, and run the pipeline with those ids.

This means that all decorated transforms will receive the same set of ids and
run in an undefined order.  This may cause issues if your pipeline consists of
multiple transforms that have to run in order, or where one transform depends
on the value of ``payload``.
