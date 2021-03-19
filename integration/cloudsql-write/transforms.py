# Copyright 2021 Spotify AB
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

import apache_beam as beam

from klio.transforms import decorators


class HelloKlio(beam.DoFn):
    """A simple DoFn."""

    @decorators.handle_klio
    def process(self, data):
        """Main entrypoint to a transform.

        Any errors raised here (explicitly or not), Klio will catch and
        log + drop the Klio message.

        For information on the Klio message, see
        https://docs.klio.io/en/latest/userguide/pipeline/message.html

        For information on yielding other information other than ``data``, see
        https://docs.klio.io/en/latest/userguide/pipeline/state.html

        Args:
            data (KlioMessage.data): The data of the Klio message, which
                contains two attributes: ``data.element`` and
                ``data.payload``.
        Yields:
            KlioMessage.data: the same Klio message data object received.
        """
        element = data.element.decode("utf-8")
        self._klio.logger.info(
            "Received '%s' from file '%s'"
            % (element, self._klio.config.job_config.events.inputs[0].file_pattern)
        )
        yield data
