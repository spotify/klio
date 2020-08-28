Message State
=============

State can be passed from one transform to the next within a pipeline using the ``data.payload``
(``bytes``) attribute. What one transform yields/returns will turn into the ``data.payload`` value
for the next transform, with three exceptions:

    1. the yielded/returned value is equal to the ``data`` argument given to the transform;
    2. the yielded/returned value is ``None``; or
    3. the transform raises an exception, therefore dropping the message.


An illustrative example:

.. code-block:: python

    # transforms.py
    import json
    import apache_beam as beam
    from klio.transforms import decorators


    class GutenTagKlio(beam.DoFn):
        # This example will call some client (e.g. a database)
        # and passes the response to the next transform
        @decorators.handle_klio
        def process(self, data):
            element = data.element.decode("utf-8")
            self._klio.logger.info(f"Received element: {element}")
            # <-- some logic -->

            response = self.some_client.query(element)
            yield response


    @decorators.handle_klio
    def filter_response(ctx, data):
        # Example that filters out KlioMessages according to the
        # message's payload (using the response data from GutenTagKlio)

        element = data.element.decode("utf-8")
        ctx.logger.info(f"Received element: {element}")

        response = data.payload.decode("utf-8")
        foo_resp_data = response.get("foo")

        # Only return an element of a PCollection that has the desired
        # response data needed
        if foo_resp_data:
            # Return value should be bytes or a str.
            foo_resp_str = json.dumps(foo_resp_data)
            return foo_resp_str.encode("utf-8")

        # For messages that do not have "foo_resp_data", they will get
        # silently dropped by Klio/Beam with an (implicit or explicit)
        # `return None`, so you may want to log any messages that were
        # filtered out for your own maintenance needs.
        ctx.logger.info(
            f"Dropping {element} because no 'foo' in response data"
        )


    class PrivetKlio(beam.DoFn):
        # Continue to process any message that had "foo" in the response
        # data (and will be available in the message payload).
        @decorators.handle_klio
        def process(self, data):
            element = data.element.decode("utf-8")
            self._klio.logger.info(f"Received element: {element}")

            foo_resp = data.payload.decode("utf-8")
            foo_resp_data = json.loads(foo_resp)

            # <-- logic -->

            # Yielding (or if a transform function, returning) `None`
            # or an object that is equal to `data` argument given to
            # this process method will actually clear out the
            # message's payload for the next transform.
            # If the next transform needs the same payload data, then
            # explicitly yield/return the same payload data (in this
            # case, `data.payload`).
            yield data
