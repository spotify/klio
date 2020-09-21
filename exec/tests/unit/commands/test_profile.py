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

import sys

import pytest

from klio_exec.commands import profile as profile_cmd


@pytest.fixture
def config(mocker):
    return mocker.Mock()


@pytest.fixture
def klio_pipeline(config):
    return profile_cmd.KlioPipeline(config)


class DummyTransformGenerator(object):
    def process(self, x):
        yield x + 10
        yield x + 20


class DummyTransformFunc(object):
    def process(self, x):
        return x + 10


class DummyTransformFuncRaises(object):
    def process(self, *args):
        raise Exception("catch me")


@pytest.fixture
def transforms():
    return [
        DummyTransformGenerator,
        DummyTransformFunc,
        DummyTransformFuncRaises,
    ]


@pytest.mark.parametrize("filename", (None, "-", "input.txt"))
def test_smart_open(filename, mocker):
    m_open = mocker.mock_open()
    mock_open = mocker.patch("klio_exec.commands.profile.open", m_open)

    with profile_cmd.smart_open(filename, fmode="r") as act_ret:
        pass

    if filename and filename != "-":
        mock_open.assert_called_once_with(filename, "r")
        assert act_ret.closed
    else:
        mock_open.assert_not_called()
        assert act_ret == sys.stdout


@pytest.mark.parametrize(
    "what,output_file,temp_out,exp_ret",
    (
        ("mem", "output.txt", False, "output.png"),
        ("mem", "output.txt", True, "klio_profile_mem_1234.png"),
        ("mem", "output", False, "output.png"),
    ),
)
def test_get_output_png_file(
    what, output_file, temp_out, exp_ret, klio_pipeline, monkeypatch
):
    monkeypatch.setattr(klio_pipeline, "_now_str", "1234")
    monkeypatch.setattr(klio_pipeline, "output_file", output_file)

    act_ret = klio_pipeline._get_output_png_file(what, temp_out)
    assert exp_ret == act_ret


@pytest.mark.parametrize(
    "output_file,plot_graph,exp_temp_output",
    ((None, True, True), (None, False, False), ("output.txt", True, False)),
)
def test_smart_temp_create(
    output_file,
    plot_graph,
    exp_temp_output,
    klio_pipeline,
    mocker,
    monkeypatch,
):
    monkeypatch.setattr(klio_pipeline, "output_file", output_file)
    mock_named_temp_file = mocker.Mock()
    mock_file = mocker.Mock()
    mock_file.name = "temp-1234"
    mock_named_temp_file.return_value = mock_file
    monkeypatch.setattr(
        profile_cmd.tempfile, "NamedTemporaryFile", mock_named_temp_file
    )

    with klio_pipeline._smart_temp_create(
        "mem", plot_graph
    ) as act_temp_output:
        pass

    assert exp_temp_output is act_temp_output
    if exp_temp_output:
        assert klio_pipeline.output_file == "temp-1234"


@pytest.mark.parametrize(
    "show_logs,input_file", ((False, None), (True, "input.txt"))
)
def test_get_subproc(
    show_logs, input_file, klio_pipeline, mocker, monkeypatch
):
    mock_subproc_open = mocker.Mock()
    monkeypatch.setattr(profile_cmd.subprocess, "Popen", mock_subproc_open)
    monkeypatch.setattr(klio_pipeline, "input_file", input_file)
    monkeypatch.setattr(klio_pipeline, "entity_ids", ("foo", "bar"))

    kwargs = {"show_logs": show_logs}
    klio_pipeline._get_subproc(**kwargs)

    exp_call_args = ["klioexec", "profile", "run-pipeline"]
    if show_logs:
        exp_call_args.append("--show-logs")
    if input_file:
        exp_call_args.extend(["--input-file", input_file])
    else:
        exp_call_args.extend(["foo", "bar"])

    mock_subproc_open.assert_called_once_with(exp_call_args)


@pytest.mark.parametrize("output_file", (None, "output.txt"))
def test_profile_wall_time_per_line(
    output_file, klio_pipeline, mocker, monkeypatch
):
    monkeypatch.setattr(klio_pipeline, "output_file", output_file)
    mock_line_prof = mocker.Mock()
    monkeypatch.setattr(
        klio_pipeline, "_get_cpu_line_profiler", mock_line_prof
    )
    mock_run_pipeline = mocker.Mock()
    monkeypatch.setattr(klio_pipeline, "_run_pipeline", mock_run_pipeline)

    klio_pipeline._profile_wall_time_per_line(iterations=1)

    if output_file:
        mock_line_prof.return_value.print_stats.assert_called_once_with(
            klio_pipeline.output_file, output_unit=1
        )
    else:
        mock_line_prof.return_value.print_stats.assert_called_once_with(
            output_unit=1
        )


@pytest.mark.parametrize("get_maximum,exp_fmode", ((True, "w"), (False, "a")))
def test_profile_memory_per_line(
    get_maximum, exp_fmode, klio_pipeline, mocker, monkeypatch
):

    mock_memory_profiler = mocker.Mock()
    monkeypatch.setattr(
        klio_pipeline, "_get_memory_line_profiler", mock_memory_profiler
    )

    mock_memory_wrapper = mocker.Mock()
    monkeypatch.setattr(
        klio_pipeline, "_get_memory_line_wrapper", mock_memory_wrapper
    )

    mock_smart_open = mocker.patch.object(profile_cmd, "smart_open")
    mock_smart_open.return_value.__enter__.return_value = "opened_file"

    mock_run_pipeline = mocker.Mock()
    monkeypatch.setattr(klio_pipeline, "_run_pipeline", mock_run_pipeline)

    mock_show_results = mocker.Mock()
    monkeypatch.setattr(
        profile_cmd.memory_profiler, "show_results", mock_show_results
    )

    klio_pipeline._profile_memory_per_line(get_maximum=get_maximum)

    mock_memory_profiler.assert_called_once_with()
    mock_memory_wrapper.assert_called_once_with(
        mock_memory_profiler.return_value, get_maximum
    )

    mock_smart_open.assert_called_once_with(
        klio_pipeline.output_file, fmode=exp_fmode
    )
    assert "opened_file" == klio_pipeline._stream
    mock_run_pipeline.assert_called_once_with()
    if get_maximum:
        mock_show_results.assert_called_once_with(
            mock_memory_profiler.return_value, stream="opened_file"
        )
    else:
        mock_show_results.assert_not_called()


@pytest.mark.parametrize("plot_graph", (True, False))
def test_profile_memory(plot_graph, klio_pipeline, mocker, monkeypatch):
    mock_get_subproc = mocker.Mock()
    monkeypatch.setattr(klio_pipeline, "_get_subproc", mock_get_subproc)
    mock_smart_temp_create = mocker.patch.object(
        klio_pipeline, "_smart_temp_create"
    )
    mock_smart_temp_create.return_value.__enter__.return_value = True
    mock_smart_open = mocker.patch.object(profile_cmd, "smart_open")
    mock_smart_open.return_value.__enter__.return_value.name = "bar"
    mock_memory_usage = mocker.Mock()
    monkeypatch.setattr(
        profile_cmd.memory_profiler, "memory_usage", mock_memory_usage
    )
    mock_get_output_png = mocker.Mock()
    monkeypatch.setattr(
        klio_pipeline, "_get_output_png_file", mock_get_output_png
    )
    mock_plot = mocker.Mock()
    monkeypatch.setattr(profile_cmd.profile_utils, "plot", mock_plot)

    kwargs = {"plot_graph": plot_graph}
    act_ret = klio_pipeline._profile_memory(**kwargs)
    if plot_graph:
        mock_get_output_png.assert_called_once_with("memory", True)
        assert mock_get_output_png.return_value == act_ret
    else:
        assert act_ret is None


@pytest.mark.parametrize("plot_graph", (True, False))
def test_profile_cpu(plot_graph, klio_pipeline, mocker, monkeypatch):
    mock_get_subproc = mocker.Mock()
    monkeypatch.setattr(klio_pipeline, "_get_subproc", mock_get_subproc)
    mock_smart_temp_create = mocker.patch.object(
        klio_pipeline, "_smart_temp_create"
    )
    mock_smart_temp_create.return_value.__enter__.return_value = True
    mock_smart_open = mocker.patch.object(profile_cmd, "smart_open")
    mock_smart_open.return_value.__enter__.return_value.name = "bar"
    mock_get_cpu_usage = mocker.Mock()
    monkeypatch.setattr(
        profile_cmd.cpu_utils, "get_cpu_usage", mock_get_cpu_usage
    )
    mock_get_output_png = mocker.Mock()
    monkeypatch.setattr(
        klio_pipeline, "_get_output_png_file", mock_get_output_png
    )
    mock_plot = mocker.Mock()
    monkeypatch.setattr(profile_cmd.profile_utils, "plot", mock_plot)

    kwargs = {"plot_graph": plot_graph}
    act_ret = klio_pipeline._profile_cpu(**kwargs)
    if plot_graph:
        mock_get_output_png.assert_called_once_with("cpu", True)
        assert mock_get_output_png.return_value == act_ret
    else:
        assert act_ret is None


@pytest.mark.parametrize("input_file", (None, "input.txt"))
def test_get_io_mapper(
    input_file, klio_pipeline, transforms, mocker, monkeypatch
):
    entity_ids = ["id1", "id2", "id3"]
    serialized_messages = [
        klio_pipeline._entity_id_to_message(i).SerializeToString()
        for i in entity_ids
    ]

    if input_file:
        monkeypatch.setattr(klio_pipeline, "input_file", input_file)
    else:
        monkeypatch.setattr(klio_pipeline, "entity_ids", entity_ids)

    mock_read_from_text = mocker.Mock()
    monkeypatch.setattr(
        profile_cmd.beam.io, "ReadFromText", mock_read_from_text
    )
    mock_create = mocker.Mock()
    monkeypatch.setattr(profile_cmd.beam, "Create", mock_create)

    mock_flatmap = mocker.Mock()
    mock_transform = mocker.MagicMock()
    mock_flatmap.return_value = mock_transform
    monkeypatch.setattr(profile_cmd.beam, "FlatMap", mock_flatmap)

    mapper = klio_pipeline._get_io_mapper(1)

    assert isinstance(mapper, profile_cmd.StubIOMapper)

    if input_file:
        mock_read_from_text.assert_called_once_with(klio_pipeline.input_file)
    else:
        mock_create.assert_called_once_with(serialized_messages)


def test_run_pipeline(klio_pipeline, mocker, monkeypatch):

    mock_get_io_mapper = mocker.Mock()
    monkeypatch.setattr(klio_pipeline, "_get_io_mapper", mock_get_io_mapper)

    mock_get_user_pipeline = mocker.Mock()
    monkeypatch.setattr(
        klio_pipeline, "_get_user_pipeline", mock_get_user_pipeline
    )

    mock_get_user_config = mocker.Mock()
    monkeypatch.setattr(
        klio_pipeline, "_get_user_config", mock_get_user_config
    )

    klio_pipeline._run_pipeline()

    mock_get_io_mapper.assert_called_once_with(1)
    mock_get_user_config.assert_called_once_with()
    mock_get_user_pipeline.assert_called_once_with(
        mock_get_user_config.return_value, mock_get_io_mapper.return_value
    )
    mock_get_user_pipeline.return_value.run.assert_called_once_with()


@pytest.mark.parametrize(
    "what", ("run", "cpu", "timeit", "memory", "memory_per_line", "foo")
)
def test_profile(what, klio_pipeline, mocker, monkeypatch):
    mock_run_pipeline = mocker.Mock()
    monkeypatch.setattr(klio_pipeline, "_run_pipeline", mock_run_pipeline)

    mock_profile_cpu = mocker.Mock()
    monkeypatch.setattr(klio_pipeline, "_profile_cpu", mock_profile_cpu)

    mock_profile_memory = mocker.Mock()
    monkeypatch.setattr(klio_pipeline, "_profile_memory", mock_profile_memory)

    mock_memory_per_line = mocker.Mock()
    monkeypatch.setattr(
        klio_pipeline, "_profile_memory_per_line", mock_memory_per_line
    )

    mock_wall_time_per_line = mocker.Mock()
    monkeypatch.setattr(
        klio_pipeline, "_profile_wall_time_per_line", mock_wall_time_per_line
    )

    klio_pipeline.profile(what)

    if what == "run":
        mock_run_pipeline.assert_called_once_with()
        mock_profile_cpu.assert_not_called()
        mock_profile_memory.assert_not_called()
        mock_memory_per_line.assert_not_called()
        mock_wall_time_per_line.assert_not_called()

    elif what == "cpu":
        mock_run_pipeline.assert_not_called()
        mock_profile_cpu.assert_called_once_with()
        mock_profile_memory.assert_not_called()
        mock_memory_per_line.assert_not_called()
        mock_wall_time_per_line.assert_not_called()

    elif what == "memory":
        mock_run_pipeline.assert_not_called()
        mock_profile_cpu.assert_not_called()
        mock_profile_memory.assert_called_once_with()
        mock_memory_per_line.assert_not_called()
        mock_wall_time_per_line.assert_not_called()

    elif what == "memory_per_line":
        mock_run_pipeline.assert_not_called()
        mock_profile_cpu.assert_not_called()
        mock_profile_memory.assert_not_called()
        mock_memory_per_line.assert_called_once_with()
        mock_wall_time_per_line.assert_not_called()

    elif what == "timeit":
        mock_run_pipeline.assert_not_called()
        mock_profile_cpu.assert_not_called()
        mock_profile_memory.assert_not_called()
        mock_memory_per_line.assert_not_called()
        mock_wall_time_per_line.assert_called_once_with()

    else:
        mock_run_pipeline.assert_not_called()
        mock_profile_cpu.assert_not_called()
        mock_profile_memory.assert_not_called()
        mock_memory_per_line.assert_not_called()
        mock_wall_time_per_line.assert_not_called()
