Vocal Separation & Spectrograms
===============================

The following example takes the `vocal separation tutorial <https://librosa.org/doc/latest/
auto_examples/plot_vocal_separation.html>`_ from `librosa <https://librosa.org>`_ and adapts it to
a Klio pipeline.

.. admonition:: Refer to the full example
    :class: caution

    This example **only** highlights a Klio job's ``transforms.py`` and ``run.py``, but a fully
    functional Klio job requires other files as well.

    For the full example, please refer to the `code in the examples directory <https://github.com/spotify/klio/tree/master/examples/audio_spectrograms>`_.


.. _audio-ktransforms:

Klio-ified Transforms
---------------------

Audio-related Transforms
~~~~~~~~~~~~~~~~~~~~~~~~

.. _get-magnitude:

``GetMagnitude``
^^^^^^^^^^^^^^^^

Using :func:`librosa.magphase`, compute the spectrogram magnitude and phase (but only use the
spectrogram magnitude).


.. literalinclude:: ../../../../examples/audio_spectrograms/transforms.py
   :language: python
   :pyobject: GetMagnitude
   :caption: `examples/audio_spectrograms/transforms.py`_


.. admonition:: Using Klio decorators
    :class: important

    Notice that we're making use of two decorators: :func:`@tfm_decorators.handle_klio
    <klio.transforms.decorators.handle_klio>` and :func:`@audio_decorators.handle_binary
    <klio_audio.decorators.handle_binary>`.

    :func:`@tfm_decorators.handle_klio <klio.transforms.decorators.handle_klio>` will handle the
    required :ref:`de/serialization <serialization-klio-message>` of incoming and outgoing
    :ref:`KlioMessages <klio-message>` as well as attach the :ref:`KlioContext <kliocontext>`
    object to the transform's instance, making it accessible via ``self._klio``.

    Since the ``payload`` of the incoming ``KlioMessage`` is expected to be a serialized
    :class:`numpy.ndarray`, the :func:`@audio_decorators.handle_binary
    <klio_audio.decorators.handle_binary>` decorator will take that ``payload`` and handle the
    unserialization to a :class:`numpy.ndarray` object. Here we're telling ``@audio_decorators.handle_binary`` to load with :func:`numpy <numpy.load>` instead of the
    default :func:`pickle.load` in order to give us better memory performance.

.. admonition:: Using tagged outputs
    :class: important

    The two ``yield`` statements in ``GetMagnitude`` makes use of Apache Beam's support for `tagged
    outputs <https://beam.apache.org/documentation/programming-guide/#additional-outputs>`_. In the
    ``run`` function of our ``run.py`` file, we'll :ref:`make use<tagged-in-use>` of the tagged
    outputs to only work with what we need, the ``spectrogram``, not the ``phase``. Tagged outputs
    can also be used for branching within a pipeline (like if we wanted to do something different
    to the ``phase`` value).


.. _filter-nn:

``FilterNearestNeighbors``
^^^^^^^^^^^^^^^^^^^^^^^^^^

Using :func:`librosa.decompose.nn_filter`, given the spectrogram from :ref:`get-magnitude`, filter
the nearest neighbors. From librosa's `tutorial <https://librosa.org/doc/latest/auto_examples/plot_vocal_separation.html>`_:

    We'll compare frames using cosine similarity, and aggregate similar frames
    by taking their (per-frequency) median value.

    To avoid being biased by local continuity, we constrain similar frames to be
    separated by at least 2 seconds.

    This suppresses sparse/non-repetitive deviations from the average spectrum,
    and works well to discard vocal elements.

.. literalinclude:: ../../../../examples/audio_spectrograms/transforms.py
   :language: python
   :pyobject: FilterNearestNeighbors
   :caption: `examples/audio_spectrograms/transforms.py`_


.. admonition:: Using the same decorators slightly differently

    Notice that we're making use of two decorators: :func:`@tfm_decorators.handle_klio
    <klio.transforms.decorators.handle_klio>` and :func:`@audio_decorators.handle_binary
    <klio_audio.decorators.handle_binary>`.

    :func:`@tfm_decorators.handle_klio <klio.transforms.decorators.handle_klio>` will handle the
    required :ref:`de/serialization <serialization-klio-message>` of incoming and outgoing
    :ref:`KlioMessages <klio-message>` as well as attach the :ref:`KlioContext <kliocontext>`
    object to the transform's instance, making it accessible via ``self._klio``.

    Since the ``payload`` of the incoming ``KlioMessage`` is expected to be a serialized
    :class:`numpy.ndarray`, the :func:`@audio_decorators.handle_binary
    <klio_audio.decorators.handle_binary>` decorator will take that ``payload`` and handle the
    unserialization to a :class:`numpy.ndarray` object. Here we're telling ``@audio_decorators.handle_binary`` to load with :func:`numpy <numpy.load>` instead of the
    default :func:`pickle.load` in order to give us better memory performance.

.. _soft-mask:

``GetSoftMask``
^^^^^^^^^^^^^^^

Given the output of :ref:`get-magnitude` and :ref:`filter-nn`, generate masks using :func:`librosa.util.softmask`:

.. literalinclude:: ../../../../examples/audio_spectrograms/transforms.py
   :language: python
   :pyobject: GetSoftMask
   :caption: `examples/audio_spectrograms/transforms.py`_


Helper Transforms
~~~~~~~~~~~~~~~~~

The following transforms are simple functions to be used with :func:`apache_beam.Map
<apache_beam.transforms.core.Map>` (see :ref:`def-audio-pipeline-ex` below for how its used).

``create_key_from_element`` is used to take a ``PCollection`` of ``KlioMessages`` and transform
it into a ``PCollection`` of ``(key, KlioMessage)`` pairings, where the key is the
:ref:`data.element <data>` of the ``KlioMessage``. This will be helpful for when we need to pair
up (:class:`apache_beam.CoGroupByKey <apache_beam.transforms.util.CoGroupByKey>`) outputs of the
same audio file.

.. literalinclude:: ../../../../examples/audio_spectrograms/transforms.py
   :language: python
   :pyobject: create_key_from_element
   :caption: `examples/audio_spectrograms/transforms.py`_

.. _subtract-filter:

The next helper transform, ``subtract_filter_from_full``, takes a ``(key, KlioMessage)`` pairing
and calculates the difference between the full spectrogram and the nearest neighbors filtered
spectrogram. This will be used to help calculate the masks.

.. literalinclude:: ../../../../examples/audio_spectrograms/transforms.py
   :language: python
   :pyobject: subtract_filter_from_full
   :caption: `examples/audio_spectrograms/transforms.py`_


Helper Functions for Transforms
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

These last two functions are not transforms, just utility functions that are used in a few
transforms.

``_unpickle_from_klio_message`` takes raw bytes, serializes the bytes into a ``KlioMessage``, and
returns the ``data.payload`` of the ``KlioMessage`` unpickled.


.. literalinclude:: ../../../../examples/audio_spectrograms/transforms.py
   :language: python
   :pyobject: _unpickle_from_klio_message
   :caption: `examples/audio_spectrograms/transforms.py`_

The next helper function, ``_dump_to_klio_message``,  creates a ``KlioMessage`` given a key and
payload data. It also serializes the payload data with :func:`numpy.save` before returning the
``KlioMessage`` as raw bytes.

.. literalinclude:: ../../../../examples/audio_spectrograms/transforms.py
   :language: python
   :pyobject: _dump_to_klio_message
   :caption: `examples/audio_spectrograms/transforms.py`_


Full ``transforms.py`` Example
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. collapsible:: Full ``transforms.py`` example

    .. literalinclude:: ../../../../examples/audio_spectrograms/transforms.py
       :language: python
       :caption: `examples/audio_spectrograms/transforms.py`_



.. _def-audio-pipeline-ex:

Defining a Klio Pipeline
------------------------

Below is a walk-through on the actual construction of the Klio-ified pipeline.

In the ``run.py`` module, the Klio pipeline is constructed within the ``run`` function. Klio takes
care of the reading from the configured event input, and passes in the :class:`PCollection
<apache_beam.pvalue.PCollection>` of :ref:`KlioMessages <klio-message>`.

First, our imports, including importing our :ref:`audio transforms <audio-ktransforms>` from above:

.. literalinclude:: ../../../../examples/audio_spectrograms/run.py
   :language: python
   :caption: `examples/audio_spectrograms/run.py`_
   :start-at: import apache_beam
   :end-at: from audio_spectrograms


We then begin constructing our pipeline by using a few helper transforms provided by the
``klio-audio`` package:

1. :class:`GcsLoadBinary <klio_audio.transforms.io.GcsLoadBinary>` to download audio from GCS into memory;
2. :class:`LoadAudio <klio_audio.transforms.audio.LoadAudio>` - a wrapper transform around :func:`librosa.load` - to load 5 seconds of the audio as a :class:`np.ndarray <numpy.ndarray>`; then
3. compute the :func:`stft <librosa.stft>` with :class:`GetSTFT <klio_audio.transforms.audio.GetSTFT>`.

.. literalinclude:: ../../../../examples/audio_spectrograms/run.py
   :language: python
   :caption: `examples/audio_spectrograms/run.py`_
   :start-at: def run
   :end-before: get magnitude

We then use the computed ``stft`` to get the magnitude of the audio with our :ref:`GetMagnitude
<get-magnitude>` transform:

.. literalinclude:: ../../../../examples/audio_spectrograms/run.py
   :language: python
   :caption: `examples/audio_spectrograms/run.py`_
   :start-after: get magnitude
   :end-before: map the result

.. _tagged-in-use:

Next we need to map the spectrogram result to a key (which will be``KlioMessage.data.element``) so
we can group all results by key.


.. literalinclude:: ../../../../examples/audio_spectrograms/run.py
   :language: python
   :caption: `examples/audio_spectrograms/run.py`_
   :start-at: magnitude_key
   :end-before: get nearest

.. admonition:: Using tagged outputs

    Notice how instead of ``magnitude``, we're piping ``magnitude.spectrogram`` to the transform
    ``beam.Map(transforms.create_key_from_element)``. This is because the ``GetMagnitude()``
    transform has two outputs, both of which are "tagged". Beam allows transforms to return more
    than one output by way of `tagging <https://beam.apache.org/documentation/programming-guide/
    #additional-outputs>`_. The tags used in :class:`apache_beam.pvalue.TaggedOutput` in the yield
    statements of ``GetMagnitude()`` turn into attributes on the ``PCollection`` themselves.

    .. code-block::

        pvalue.TaggedOutput("spectrogram") -> magnitude.spectrogram
        pvalue.TaggedOutput("phase") -> magnitude.phase


Now, we generate the nearest neighbors filter with the spectrogram and also map its result to a
key (``KlioMessage.data.element``).

.. literalinclude:: ../../../../examples/audio_spectrograms/run.py
   :language: python
   :caption: `examples/audio_spectrograms/run.py`_
   :start-at: nn_filter
   :end-before: map together


Map together the magnitude spectrogram with its nearest neighbor by key.

.. literalinclude:: ../../../../examples/audio_spectrograms/run.py
   :language: python
   :caption: `examples/audio_spectrograms/run.py`_
   :start-at: merge
   :end-before: calc

Take the grouped ``PCollection`` to then :ref:`find the difference between the full spectrogram
and the filtered one <subtract-filter>`.

.. literalinclude:: ../../../../examples/audio_spectrograms/run.py
   :language: python
   :caption: `examples/audio_spectrograms/run.py`_
   :start-at: net =
   :end-before: create a mask

Create a mask with our :ref:`soft-mask` transform.

.. literalinclude:: ../../../../examples/audio_spectrograms/run.py
   :language: python
   :caption: `examples/audio_spectrograms/run.py`_
   :start-at: first_mask
   :end-before: create

And another mask - essentially the inverse of what we just did:

.. literalinclude:: ../../../../examples/audio_spectrograms/run.py
   :language: python
   :caption: `examples/audio_spectrograms/run.py`_
   :start-at: second_mask
   :end-before: plot

Then generate three sets of output files. First, the plot of the magnitude spectrogram:

.. literalinclude:: ../../../../examples/audio_spectrograms/run.py
   :language: python
   :caption: `examples/audio_spectrograms/run.py`_
   :start-at: magnitude_out
   :end-before: plot the first mask

Second, the spectrogram plot of the first mask:

.. literalinclude:: ../../../../examples/audio_spectrograms/run.py
   :language: python
   :caption: `examples/audio_spectrograms/run.py`_
   :start-at: background_out
   :end-before: plot the second mask

And third, the spectrogram plot of the second mask:

.. literalinclude:: ../../../../examples/audio_spectrograms/run.py
   :language: python
   :caption: `examples/audio_spectrograms/run.py`_
   :start-at: foreground_out
   :end-before: flatten


Finally, we need to flatten the output into a single :class:`PCollection
<apache_beam.pvalue.PCollection>` of :ref:`KlioMessages <klio-message>` (as Klio is not yet able to
handle multiple output `PCollections` to multiple event outputs), as well as remove duplicate
``KlioMessages`` since there are now three "forks" of PCollections: the full spectrogram, the
background mask, and the foreground mask.

.. literalinclude:: ../../../../examples/audio_spectrograms/run.py
   :language: python
   :caption: `examples/audio_spectrograms/run.py`_
   :start-at: out_pcol

Full ``run.py`` Example
~~~~~~~~~~~~~~~~~~~~~~~

.. collapsible:: Full ``run.py`` example

    .. literalinclude:: ../../../../examples/audio_spectrograms/run.py
       :caption: `examples/audio_spectrograms/run.py`_
       :language: python




.. _examples/audio_spectrograms/transforms.py: https://github.com/spotify/klio/tree/master/examples/audio_spectrograms/transforms.py
.. _examples/audio_spectrograms/run.py: https://github.com/spotify/klio/tree/master/examples/audio_spectrograms/run.py
