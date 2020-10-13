Why not other open source frameworks?
=====================================

There are a number of well-developed, supported data processing frameworks available in the open.
At Spotify, we've standardized around `Apache Beam <https://beam.apache.org/>`_ with our sister open source framework, `Scio <https://spotify.github.io/scio/>`_.
We've found that Beam is a framework that engineers and researchers alike can pick up quickly to create `embarrassingly parallel <https://en.wikipedia.org/wiki/Embarrassingly_parallel>`_ pipelines.
But no solution yet existed to handle the resource and environment demands of processing media.

