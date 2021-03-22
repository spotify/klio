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

import datetime
import os

import pytest

from klio_core import config as kconfig

from klio_cli import cli
from klio_cli.commands.job import profile


@pytest.fixture
def mock_client(mocker, monkeypatch):
    mock = mocker.Mock()
    monkeypatch.setattr(profile.storage, "Client", mock)
    return mock


@pytest.fixture
def collector():
    return profile.DataflowProfileStatsCollector(
        "gs://foo/bar",
        since="Dec 26 2020 13:00 UTC",
        until="Dec 26 2020 14:00 UTC",
    )


@pytest.fixture
def mock_os_environ(mocker):
    patch = {"HOME": "/home", "USER": "cookiemonster"}
    return mocker.patch.dict(profile.os.environ, patch)


@pytest.fixture
def config_data():
    return {
        "job_name": "test-job",
        "pipeline_options": {
            "worker_harness_container_image": "gcr.register.io/squad/feature",
            "project": "test-project",
            "region": "boonies",
            "staging_location": "gs://somewhere/over/the/rainbow",
            "temp_location": "gs://somewhere/over/the/rainbow",
        },
        "job_config": {
            "inputs": [
                {
                    "topic": "foo-topic",
                    "subscription": "foo-sub",
                    "data_location": "foo-input-location",
                }
            ],
            "outputs": [
                {
                    "topic": "foo-topic-output",
                    "data_location": "foo-output-location",
                }
            ],
        },
    }


@pytest.fixture
def config(config_data):
    return kconfig.KlioConfig(config_data)


@pytest.fixture
def klio_pipeline(mock_os_environ, config, mocker, monkeypatch):
    job_dir = "/test/dir/jobs/test_run_job"
    runtime_config = cli.DockerRuntimeConfig(
        image_tag="foo-1234", force_build=False, config_file_override=None,
    )
    profile_config = cli.ProfileConfig(
        input_file="input.txt",
        output_file="output.txt",
        show_logs=False,
        entity_ids=(),
    )
    return profile.ProfilePipeline(
        job_dir, config, runtime_config, profile_config
    )


@pytest.mark.parametrize("project", (None, "test-project"))
def test_get_environment(monkeypatch, project, config_data, klio_pipeline):
    config_data["pipeline_options"]["project"] = project
    config = kconfig.KlioConfig(config_data)
    monkeypatch.setattr(klio_pipeline, "klio_config", config)
    gcreds = "/usr/gcloud/application_default_credentials.json"
    exp_envs = {
        "PYTHONPATH": "/usr/src/app",
        "GOOGLE_APPLICATION_CREDENTIALS": gcreds,
        "USER": "cookiemonster",
    }
    if project:
        exp_envs["GOOGLE_CLOUD_PROJECT"] = project

    actual_env = klio_pipeline._get_environment()

    assert exp_envs == actual_env


@pytest.mark.parametrize(
    "iterations,exp_iterations_flags",
    ((None, []), (10, ["--iterations", "10"])),
)
def test_parse_timeout_flags(klio_pipeline, iterations, exp_iterations_flags):

    subcommand_flags = {"iterations": iterations}

    exp_subcommand = ["timeit"]
    exp_subcommand.extend(exp_iterations_flags)

    actual_subcommand = klio_pipeline._parse_timeit_flags(subcommand_flags)
    assert sorted(exp_subcommand) == sorted(actual_subcommand)


@pytest.mark.parametrize(
    "maximum,per_element,exp_flags",
    ((False, True, ["--per-element"]), (True, False, ["--maximum"])),
)
def test_parse_memory_per_line_flags(
    klio_pipeline, maximum, per_element, exp_flags,
):

    subcommand_flags = {
        "get_maximum": maximum,
        "per_element": per_element,
    }

    exp_subcommand = ["memory-per-line"]
    exp_subcommand.extend(exp_flags)

    actual_subcommand = klio_pipeline._parse_memory_per_line_flags(
        subcommand_flags
    )
    assert sorted(exp_subcommand) == sorted(actual_subcommand)


@pytest.mark.parametrize(
    "interval,exp_interval_flags", ((None, []), (0.5, ["--interval", "0.5"]))
)
@pytest.mark.parametrize(
    "incl_chd,exp_incl_chd_flags", ((None, []), (True, ["--include-children"]))
)
@pytest.mark.parametrize(
    "multiproc,exp_multiproc_flags", ((None, []), (True, ["--multiprocess"]))
)
@pytest.mark.parametrize(
    "plot,exp_plot_flags", ((None, []), (True, ["--plot-graph"]))
)
def test_parse_memory_flags(
    klio_pipeline,
    interval,
    exp_interval_flags,
    incl_chd,
    exp_incl_chd_flags,
    multiproc,
    exp_multiproc_flags,
    plot,
    exp_plot_flags,
):

    subcommand_flags = {
        "interval": interval,
        "include_children": incl_chd,
        "multiprocess": multiproc,
        "plot_graph": plot,
    }

    exp_subcommand = ["memory"]
    exp_subcommand.extend(exp_interval_flags)
    exp_subcommand.extend(exp_incl_chd_flags)
    exp_subcommand.extend(exp_multiproc_flags)
    exp_subcommand.extend(exp_plot_flags)

    actual_subcommand = klio_pipeline._parse_memory_flags(subcommand_flags)
    assert sorted(exp_subcommand) == sorted(actual_subcommand)


@pytest.mark.parametrize(
    "interval,exp_interval_flags", ((None, []), (0.5, ["--interval", "0.5"]))
)
@pytest.mark.parametrize(
    "plot,exp_plot_flags", ((None, []), (True, ["--plot-graph"]))
)
def test_parse_cpu_flags(
    klio_pipeline, interval, exp_interval_flags, plot, exp_plot_flags,
):

    subcommand_flags = {
        "interval": interval,
        "plot_graph": plot,
    }

    exp_subcommand = ["cpu"]
    exp_subcommand.extend(exp_interval_flags)
    exp_subcommand.extend(exp_plot_flags)

    actual_subcommand = klio_pipeline._parse_cpu_flags(subcommand_flags)
    assert sorted(exp_subcommand) == sorted(actual_subcommand)


@pytest.mark.parametrize(
    "input_file,exp_input_file_flags,entity_ids,exp_entity_id_args",
    (
        (None, [], ["foo", "bar"], ["foo", "bar"]),
        ("input.txt", ["--input-file", "input.txt"], [], []),
    ),
)
@pytest.mark.parametrize(
    "output_file,exp_output_file_flags",
    ((None, []), ("output.txt", ["--output-file", "output.txt"])),
)
@pytest.mark.parametrize(
    "show_logs,exp_show_logs_flags", ((None, []), (True, ["--show-logs"]))
)
@pytest.mark.parametrize(
    "subcommand", ("cpu", "memory", "memory-per-line", "timeit")
)
def test_get_command(
    klio_pipeline,
    input_file,
    exp_input_file_flags,
    output_file,
    exp_output_file_flags,
    show_logs,
    exp_show_logs_flags,
    entity_ids,
    exp_entity_id_args,
    subcommand,
    mocker,
    monkeypatch,
):

    mock_parse_cpu_flags = mocker.Mock()
    mock_parse_cpu_flags.return_value = ["cpu"]
    monkeypatch.setattr(
        klio_pipeline, "_parse_cpu_flags", mock_parse_cpu_flags
    )
    mock_parse_memory_flags = mocker.Mock()
    mock_parse_memory_flags.return_value = ["memory"]
    monkeypatch.setattr(
        klio_pipeline, "_parse_memory_flags", mock_parse_memory_flags
    )
    mock_parse_memory_per_line_flags = mocker.Mock()
    mock_parse_memory_per_line_flags.return_value = ["memory-per-line"]
    monkeypatch.setattr(
        klio_pipeline,
        "_parse_memory_per_line_flags",
        mock_parse_memory_per_line_flags,
    )
    mock_parse_timeit_flags = mocker.Mock()
    mock_parse_timeit_flags.return_value = ["timeit"]
    monkeypatch.setattr(
        klio_pipeline, "_parse_timeit_flags", mock_parse_timeit_flags
    )
    profile_conf = cli.ProfileConfig(
        input_file=input_file,
        output_file=output_file,
        entity_ids=entity_ids,
        show_logs=show_logs,
    )
    monkeypatch.setattr(klio_pipeline, "profile_config", profile_conf)

    exp_command = ["profile"]
    if subcommand:
        exp_command.append(subcommand)
    exp_command.extend(exp_input_file_flags)
    exp_command.extend(exp_output_file_flags)
    exp_command.extend(exp_show_logs_flags)
    exp_command.extend(exp_entity_id_args)

    actual_command = klio_pipeline._get_command(subcommand, {})

    assert sorted(exp_command) == sorted(actual_command)

    if subcommand == "cpu":
        mock_parse_cpu_flags.assert_called_once_with({})
        mock_parse_memory_flags.assert_not_called()
        mock_parse_memory_per_line_flags.assert_not_called()
        mock_parse_timeit_flags.assert_not_called()

    elif subcommand == "memory":
        mock_parse_cpu_flags.assert_not_called()
        mock_parse_memory_flags.assert_called_once_with({})
        mock_parse_memory_per_line_flags.assert_not_called()
        mock_parse_timeit_flags.assert_not_called()

    elif subcommand == "memory-per-line":
        mock_parse_cpu_flags.assert_not_called()
        mock_parse_memory_flags.assert_not_called()
        mock_parse_memory_per_line_flags.assert_called_once_with({})
        mock_parse_timeit_flags.assert_not_called()

    elif subcommand == "timeit":
        mock_parse_cpu_flags.assert_not_called()
        mock_parse_memory_flags.assert_not_called()
        mock_parse_memory_per_line_flags.assert_not_called()
        mock_parse_timeit_flags.assert_called_once_with({})

    else:
        mock_parse_cpu_flags.assert_not_called()
        mock_parse_memory_flags.assert_not_called()
        mock_parse_memory_per_line_flags.assert_not_called()
        mock_parse_timeit_flags.assert_not_called()


def test_run(klio_pipeline, mocker, monkeypatch):
    mock_check_docker_setup = mocker.Mock()
    monkeypatch.setattr(
        klio_pipeline, "_check_docker_setup", mock_check_docker_setup
    )
    mock_setup_docker_image = mocker.Mock()
    monkeypatch.setattr(
        klio_pipeline, "_setup_docker_image", mock_setup_docker_image
    )
    mock_get_docker_runflags = mocker.Mock()
    monkeypatch.setattr(
        klio_pipeline, "_get_docker_runflags", mock_get_docker_runflags
    )
    mock_run_docker_container = mocker.Mock()
    monkeypatch.setattr(
        klio_pipeline, "_run_docker_container", mock_run_docker_container
    )
    mock_write_effective_config = mocker.Mock()
    monkeypatch.setattr(
        klio_pipeline, "_write_effective_config", mock_write_effective_config
    )

    klio_pipeline.run(what="what", subcommand_flags={"some": "flags"})

    mock_check_docker_setup.assert_called_once_with()
    mock_setup_docker_image.assert_called_once_with()
    mock_get_docker_runflags.assert_called_once_with(
        subcommand="what", subcommand_flags={"some": "flags"}
    )
    mock_run_docker_container.assert_called_once_with(
        mock_get_docker_runflags.return_value
    )
    mock_write_effective_config.assert_called_once_with()


@pytest.mark.parametrize("input_file", (None, "foo.pstats"))
def test_init_collector(input_file, mocker, monkeypatch):
    mock_parse_date = mocker.Mock()
    monkeypatch.setattr(
        profile.DataflowProfileStatsCollector, "_parse_date", mock_parse_date
    )

    profile.DataflowProfileStatsCollector(
        "gs://foo/bar",
        input_file=input_file,
        since="Dec 26 2020 13:00 UTC",
        until="Dec 26 2020 14:00 UTC",
    )
    if input_file:
        mock_parse_date.assert_not_called()
    else:
        assert 2 == mock_parse_date.call_count
        exp_calls = [
            mocker.call("Dec 26 2020 13:00 UTC"),
            mocker.call("Dec 26 2020 14:00 UTC"),
        ]
        assert exp_calls == mock_parse_date.call_args_list


def test_parse_date(mocker, monkeypatch):
    mock_dateparser = mocker.Mock()
    monkeypatch.setattr(profile, "dateparser", mock_dateparser)

    profile.DataflowProfileStatsCollector._parse_date("1 hour ago")

    mock_dateparser.parse.assert_called_once_with(
        "1 hour ago",
        settings={"TO_TIMEZONE": "UTC", "RETURN_AS_TIMEZONE_AWARE": True},
    )


@pytest.mark.parametrize(
    "restrictions,exp_restrictions",
    (
        (["foo"], ["foo"]),
        (["0.1"], [0.1]),
        (["20"], [20]),
        (["foo.py"], ["foo.py"]),
        (["foo", "0.1", "20", "foo.py"], ["foo", 0.1, 20, "foo.py"]),
    ),
)
def test_clean_restrictions(restrictions, exp_restrictions):
    actual = profile.DataflowProfileStatsCollector._clean_restrictions(
        restrictions
    )

    assert exp_restrictions == list(actual)


def test_get_gcs_blobs(collector, mock_client, mocker):
    blob_before_range = mocker.Mock(
        time_created=datetime.datetime(
            2020, 12, 26, 12, 40, tzinfo=datetime.timezone.utc
        )
    )
    blob_within_range = mocker.Mock(
        time_created=datetime.datetime(
            2020, 12, 26, 13, 40, tzinfo=datetime.timezone.utc
        )
    )
    blob_after_range = mocker.Mock(
        time_created=datetime.datetime(
            2020, 12, 26, 14, 40, tzinfo=datetime.timezone.utc
        )
    )
    bucket = mocker.Mock()
    bucket.list_blobs.return_value = (
        blob_before_range,
        blob_within_range,
        blob_after_range,
    )
    mock_client.return_value.get_bucket.return_value = bucket

    blobs = list(collector._get_gcs_blobs())

    bucket.list_blobs.assert_called_once_with(prefix="bar")
    assert 1 == len(blobs)
    assert blob_within_range == blobs[0]


def test_get_gcs_blobs_bucket_raises(collector, mock_client, caplog):
    mock_client.return_value.get_bucket.side_effect = Exception(
        "404 not found"
    )

    with pytest.raises(SystemExit, match="1"):
        list(collector._get_gcs_blobs())

    assert 1 == len(caplog.records)
    assert "ERROR" in caplog.records[0].levelname
    assert "Error getting bucket" in caplog.records[0].message


def test_get_gcs_blobs_list_blobs_raises(
    collector, mock_client, mocker, caplog
):
    blob_within_range = mocker.Mock(
        time_created=datetime.datetime(
            2020, 12, 26, 13, 40, tzinfo=datetime.timezone.utc
        )
    )

    def _blob_generator(*args, **kwargs):
        yield blob_within_range
        raise Exception("foo")

    bucket = mocker.Mock()
    bucket.list_blobs.return_value = _blob_generator()
    mock_client.return_value.get_bucket.return_value = bucket

    with pytest.raises(SystemExit, match="1"):
        list(collector._get_gcs_blobs())

    assert 1 == len(caplog.records)
    assert "ERROR" in caplog.records[0].levelname
    assert "Error getting profiling data" in caplog.records[0].message


def test_download_gcs_file(collector, mocker, tmpdir):
    blob_to_download = mocker.Mock()
    blob_to_download.name = "foobar"

    dest = tmpdir.mkdir("testing")

    ret_temp_file = collector._download_gcs_file(
        blob_to_download, dest.strpath
    )

    exp_output_file = os.path.join(dest.strpath, blob_to_download.name)

    assert exp_output_file == ret_temp_file


def test_download_gcs_file_raises(collector, mocker, tmpdir, caplog):
    blob_to_download = mocker.Mock()
    blob_to_download.name = "foobar"
    blob_to_download.download_to_file.side_effect = Exception("foo")

    dest = tmpdir.mkdir("testing")

    with pytest.raises(SystemExit, match="1"):
        collector._download_gcs_file(blob_to_download, dest.strpath)

    assert 1 == len(caplog.records)
    assert "ERROR" in caplog.records[0].levelname
    assert "Error downloading" in caplog.records[0].message


def test_get_stats_object(collector, mocker, monkeypatch):
    blob = mocker.Mock()
    mock_get_blobs = mocker.Mock()
    mock_get_blobs.return_value = [
        blob,
    ]
    monkeypatch.setattr(collector, "_get_gcs_blobs", mock_get_blobs)

    mock_tempdir = mocker.Mock()
    mock_tempdir.name = "./klio-gcs-profile-data-1234"
    mock_create_tempdir = mocker.Mock()
    mock_create_tempdir.return_value = mock_tempdir
    monkeypatch.setattr(
        profile.tempfile, "TemporaryDirectory", mock_create_tempdir
    )

    mock_download_gcs_file = mocker.Mock()
    mock_download_gcs_file.return_value = mock_tempdir.name + "/foo"
    monkeypatch.setattr(
        collector, "_download_gcs_file", mock_download_gcs_file
    )

    mock_stats = mocker.Mock()
    monkeypatch.setattr(profile.pstats, "Stats", mock_stats)

    act_stats = collector._get_stats_object()

    assert mock_stats.return_value == act_stats
    mock_get_blobs.assert_called_once_with()
    mock_download_gcs_file.assert_called_once_with(blob, mock_tempdir.name)
    mock_stats.assert_called_once_with(mock_download_gcs_file.return_value)


def test_get_stats_object_input_file(collector, mocker, monkeypatch):
    mock_stats = mocker.Mock()
    monkeypatch.setattr(profile.pstats, "Stats", mock_stats)

    monkeypatch.setattr(collector, "input_file", "foo.pstats")

    mock_get_blobs = mocker.Mock()
    monkeypatch.setattr(collector, "_get_gcs_blobs", mock_get_blobs)
    mock_download_gcs_file = mocker.Mock()
    monkeypatch.setattr(
        collector, "_download_gcs_file", mock_download_gcs_file
    )

    act_stats = collector._get_stats_object()

    assert mock_stats.return_value == act_stats
    mock_stats.assert_called_once_with("foo.pstats")
    mock_get_blobs.assert_not_called()
    mock_download_gcs_file.assert_not_called()


@pytest.mark.parametrize("output_file", (None, "foo.pstats"))
def test_get(output_file, collector, mocker, monkeypatch):
    mock_restrictions = mocker.Mock()
    mock_restrictions.return_value = [
        None,
    ]
    monkeypatch.setattr(collector, "_clean_restrictions", mock_restrictions)

    mock_get_stats = mocker.Mock()
    monkeypatch.setattr(collector, "_get_stats_object", mock_get_stats)

    if output_file:
        monkeypatch.setattr(collector, "output_file", output_file)

    sort_stats = [
        "tottime",
    ]
    restrictions = [
        None,
    ]
    collector.get(sort_stats, restrictions)

    mock_restrictions.assert_called_once_with(restrictions)
    mock_get_stats.assert_called_once_with()
    if not output_file:
        mock_get_stats.return_value.dump_stats.assert_not_called()
        mock_get_stats.return_value.sort_stats.assert_called_once_with(
            "tottime"
        )
        mock_get_stats.return_value.print_stats.assert_called_once_with(None)
    else:
        mock_get_stats.return_value.dump_stats.assert_called_once_with(
            collector.output_file
        )
