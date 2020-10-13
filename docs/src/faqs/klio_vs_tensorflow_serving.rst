How does Klio compare to Tensorflow Serving?
============================================

`Tensorflow Serving <https://www.tensorflow.org/tfx/guide/serving>`_ enables creating a service around a Tensorflow-based ML model.
Although a streaming Klio job could be compared to serving a model with a service, Klio is meant for media processing pipelines, not necessarily serving a model.
Klio enables heavy file I/O for processing media, whether it's using a model or not.
As well, Klio is agnostic to the type of ML model used (Tensorflow, PyTorch, scikit-learn, etc.).
