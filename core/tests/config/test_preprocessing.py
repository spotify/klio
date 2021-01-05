# Copyright 2020 Spotify AB
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

import json

import pytest
import yaml

from klio_core import exceptions
from klio_core.config import _preprocessing


@pytest.fixture
def kcp():
    return _preprocessing.KlioConfigPreprocessor


@pytest.fixture
def job_raw_config():
    return {
        "allow_non_klio_messages": False,
        "events": {
            "inputs": {
                "file0": {
                    "type": "file",
                    "location": "gs://test/yesterday.txt",
                },
                "file1": {"type": "file", "location": "gs://test/today.txt"},
            }
        },
    }


@pytest.fixture
def job_raw_config_templated():
    return {
        "events": {
            "inputs": {
                "bigquery0": {
                    "type": "bigquery",
                    "table": "testing.top_tracks_global",
                    "partition": "$YESTERDAY",
                },
                "bigquery1": {
                    "type": "bigquery",
                    "table": "testing.top_artists_global",
                    "partition": "$TODAY",
                },
                "bigquery2": {
                    "type": "bigquery",
                    "table": "testing.${GENRE}_join_$COUNTRY",
                    "partition": "$TODAY",
                },
            }
        }
    }


@pytest.mark.parametrize(
    "overrides_dict,expected",
    (
        # No overrides given - no changes in returned dict
        (
            {},
            {
                "allow_non_klio_messages": False,
                "events": {
                    "inputs": {
                        "file0": {
                            "type": "file",
                            "location": "gs://test/yesterday.txt",
                        },
                        "file1": {
                            "type": "file",
                            "location": "gs://test/today.txt",
                        },
                    }
                },
            },
        ),
        # Override dict given but no changes - no changes in returned dict
        (
            {"allow_non_klio_messages": True},
            {
                "allow_non_klio_messages": True,
                "events": {
                    "inputs": {
                        "file0": {
                            "type": "file",
                            "location": "gs://test/yesterday.txt",
                        },
                        "file1": {
                            "type": "file",
                            "location": "gs://test/today.txt",
                        },
                    }
                },
            },
        ),
        # Override current values
        (
            {
                "events.inputs.file0.location": "gs://test/12-31-2019.txt",
                "events.inputs.file1.location": "gs://test/01-01-2020.txt",
            },
            {
                "allow_non_klio_messages": False,
                "events": {
                    "inputs": {
                        "file0": {
                            "type": "file",
                            "location": "gs://test/12-31-2019.txt",
                        },
                        "file1": {
                            "type": "file",
                            "location": "gs://test/01-01-2020.txt",
                        },
                    }
                },
            },
        ),
        # Add new values
        (
            {
                "events.inputs.file2.location": "gs://test/01-01-2020.txt",
                "events.inputs.file2.type": "file",
            },
            {
                "allow_non_klio_messages": False,
                "events": {
                    "inputs": {
                        "file0": {
                            "type": "file",
                            "location": "gs://test/yesterday.txt",
                        },
                        "file1": {
                            "type": "file",
                            "location": "gs://test/today.txt",
                        },
                        "file2": {
                            "type": "file",
                            "location": "gs://test/01-01-2020.txt",
                        },
                    }
                },
            },
        ),
    ),
)
def test_apply_overrides(kcp, job_raw_config, overrides_dict, expected):
    # One key override
    new_dict = kcp._apply_overrides(job_raw_config, overrides_dict)
    assert expected == new_dict


@pytest.mark.parametrize(
    "template_dict,expected",
    (
        # One or more missing templates should raise a KeyError
        (
            {
                "YESTERDAY": "12-31-2019",
                "TODAY": "01-01-2020",
                "COUNTRY": "sto",
            },
            None,
        ),
        (
            {
                "YESTERDAY": "12-31-2019",
                "TODAY": "01-01-2020",
                "GENRE": "rock",
                "COUNTRY": "sto",
            },
            {
                "events": {
                    "inputs": {
                        "bigquery0": {
                            "type": "bigquery",
                            "table": "testing.top_tracks_global",
                            "partition": "12-31-2019",
                        },
                        "bigquery1": {
                            "type": "bigquery",
                            "table": "testing.top_artists_global",
                            "partition": "01-01-2020",
                        },
                        "bigquery2": {
                            "type": "bigquery",
                            "table": "testing.rock_join_sto",
                            "partition": "01-01-2020",
                        },
                    }
                }
            },
        ),
    ),
)
def test_apply_templates(
    kcp, job_raw_config_templated, template_dict, expected
):
    raw_config_str = json.dumps(job_raw_config_templated)
    if not expected:
        with pytest.raises(
            exceptions.KlioConfigTemplatingException, match=r"'GENRE' .*"
        ):
            kcp._apply_templates(raw_config_str, template_dict)
    else:
        new_dict = kcp._apply_templates(raw_config_str, template_dict)
        assert isinstance(new_dict, str)
        assert expected == json.loads(new_dict)


def test_transform_io(kcp):
    config = {
        "job_config": {
            "events": {
                "inputs": [
                    {"type": "bq", "key": "value"},
                    {"type": "bq", "key": "value2"},
                    {"type": "gcs", "gcskey": "gcsvalue"},
                ],
                "outputs": [
                    {"type": "bq", "key": "value", "name": "mybq"},
                    {"type": "bq", "key": "value2"},
                ],
            },
            "data": {
                "inputs": [],
                "outputs": [
                    {"type": "bq", "key": "value", "name": "mybq"},
                    {"type": "bq", "key": "value2"},
                ],
            },
        }
    }
    expected = {
        "job_config": {
            "events": {
                "inputs": {
                    "bq0": {"type": "bq", "key": "value"},
                    "bq1": {"type": "bq", "key": "value2"},
                    "gcs0": {"type": "gcs", "gcskey": "gcsvalue"},
                },
                "outputs": {
                    "mybq": {"type": "bq", "key": "value"},
                    "bq0": {"type": "bq", "key": "value2"},
                },
            },
            "data": {
                "inputs": {},
                "outputs": {
                    "mybq": {"type": "bq", "key": "value"},
                    "bq0": {"type": "bq", "key": "value2"},
                },
            },
        }
    }
    actual = kcp._transform_io_sections(config)

    assert actual == expected


def test_apply_plugin_preprocessing(mocker, kcp):

    config = {
        "version": 1,
        "job_name": "test_job",
        "job_config": {},
    }
    raw_config = yaml.dump(config)

    def add_field(config_dict):
        config_dict["version"] = 2
        return config_dict

    expected_config = config.copy()
    expected_config["version"] = 2

    mocker.patch.object(kcp, "PLUGIN_PREPROCESSORS", [add_field])

    processed_config = kcp.process(raw_config, [], [])

    assert expected_config == processed_config
