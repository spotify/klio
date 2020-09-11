# Copyright 2020 Spotify AB

import sys

import pytest

from klio.transforms import decorators

from klio_exec.commands import profile as profile_cmd
from klio_exec.commands.utils import profile_utils


@pytest.fixture
def klio_pipeline():
    return profile_cmd.KlioPipeline()


class DummyTransformGenerator(object):
    @decorators.profile()
    def process(self, x):
        yield x + 10
        yield x + 20


class DummyTransformFunc(object):
    @decorators.profile()
    def process(self, x):
        return x + 10


class DummyTransformFuncRaises(object):
    @decorators.profile()
    def process(self, *args):
        raise Exception("catch me")


@pytest.fixture
def transforms():
    return [
        DummyTransformGenerator,
        DummyTransformFunc,
        DummyTransformFuncRaises,
    ]


@pytest.fixture
def profile_objects():
    return [
        profile_utils.ProfileObject.from_object(
            DummyTransformGenerator.process
        ),
        profile_utils.ProfileObject.from_object(DummyTransformFunc.process),
        profile_utils.ProfileObject.from_object(
            DummyTransformFuncRaises.process
        ),
    ]


class AKlioClass(object):
    pass


class ANonKlioClass(object):
    pass


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


@pytest.mark.parametrize("get_maximum", (True, False))
def test_wrap_memory_per_line(
    get_maximum, klio_pipeline, profile_objects, mocker, monkeypatch
):
    kprof_cls = profile_cmd.memory_utils.KMemoryLineProfiler
    monkeypatch.setattr(
        kprof_cls, "wrap_per_element", lambda x, stream: "{} element".format(x)
    )
    monkeypatch.setattr(
        kprof_cls,
        "wrap_maximum",
        lambda prof, x, stream: "{} maximum".format(x),
    )

    for obj in profile_objects:
        monkeypatch.setattr(obj, "to_wrapped_transform", lambda x: x)
    monkeypatch.setattr(
        klio_pipeline, "_wrap_user_exceptions", lambda x: "[{}]".format(x)
    )

    wrapped_transforms = list(
        klio_pipeline._wrap_memory_per_line(
            profile_objects, get_maximum=get_maximum
        )
    )

    for txf in wrapped_transforms:
        if get_maximum:
            assert txf("test") == "[test maximum]"
        else:
            assert txf("test") == "[test element]"


def test_wrap_wall_time(klio_pipeline, profile_objects, mocker, monkeypatch):
    monkeypatch.setattr(klio_pipeline, "_prof", lambda x: "got {}".format(x))

    for obj in profile_objects:
        monkeypatch.setattr(obj, "to_wrapped_transform", lambda x: x)

    monkeypatch.setattr(
        klio_pipeline, "_wrap_user_exceptions", lambda x: "[{}]".format(x)
    )

    wrapped_transforms = list(klio_pipeline._wrap_wall_time(profile_objects))

    for txf in wrapped_transforms:
        assert txf("test") == "[got test]"


@pytest.mark.parametrize("output_file", (None, "output.txt"))
def test_profile_wall_time_per_line(
    output_file, klio_pipeline, transforms, mocker, monkeypatch
):
    monkeypatch.setattr(klio_pipeline, "output_file", output_file)
    mock_line_prof = mocker.Mock()
    monkeypatch.setattr(profile_cmd.cpu_utils, "KLineProfiler", mock_line_prof)
    mock_wrap_wall_time = mocker.Mock()
    monkeypatch.setattr(klio_pipeline, "_wrap_wall_time", mock_wrap_wall_time)
    mock_run_pipeline = mocker.Mock()
    monkeypatch.setattr(klio_pipeline, "_run_pipeline", mock_run_pipeline)

    klio_pipeline._profile_wall_time_per_line(transforms, iterations=1)

    assert klio_pipeline._prof == mock_line_prof.return_value

    mock_wrap_wall_time.assert_called_once_with(transforms)

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
    get_maximum, exp_fmode, klio_pipeline, transforms, mocker, monkeypatch
):
    mock_prof = mocker.Mock()
    monkeypatch.setattr(
        profile_cmd.memory_utils, "KMemoryLineProfiler", mock_prof
    )
    mock_wrap_memory_per_line = mocker.Mock()
    monkeypatch.setattr(
        klio_pipeline, "_wrap_memory_per_line", mock_wrap_memory_per_line
    )
    mock_smart_open = mocker.patch.object(profile_cmd, "smart_open")
    mock_smart_open.return_value.__enter__.return_value = "opened_file"
    mock_run_pipeline = mocker.Mock()
    monkeypatch.setattr(klio_pipeline, "_run_pipeline", mock_run_pipeline)
    mock_show_results = mocker.Mock()
    monkeypatch.setattr(
        profile_cmd.memory_profiler, "show_results", mock_show_results
    )

    klio_pipeline._profile_memory_per_line(transforms, get_maximum=get_maximum)

    assert mock_prof.return_value == klio_pipeline._prof
    mock_wrap_memory_per_line.assert_called_once_with(transforms, get_maximum)
    mock_smart_open.assert_called_once_with(
        klio_pipeline.output_file, fmode=exp_fmode
    )
    assert "opened_file" == klio_pipeline._stream
    mock_run_pipeline.assert_called_once_with(
        mock_wrap_memory_per_line.return_value
    )
    if get_maximum:
        mock_show_results.assert_called_once_with(
            mock_prof.return_value, stream="opened_file"
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
def test_run_pipeline(
    input_file, klio_pipeline, transforms, mocker, monkeypatch
):
    if input_file:
        monkeypatch.setattr(klio_pipeline, "input_file", input_file)

    mock_wrap_transforms = mocker.Mock()
    mock_wrap_transforms.return_value = transforms
    monkeypatch.setattr(
        profile_cmd.wrappers, "print_user_exceptions", mock_wrap_transforms
    )

    mock_options = mocker.Mock()
    monkeypatch.setattr(
        profile_cmd.pipeline_options, "PipelineOptions", mock_options
    )
    mock_pipeline = mocker.Mock()
    monkeypatch.setattr(profile_cmd.beam, "Pipeline", mock_pipeline)

    mock_read_from_text = mocker.Mock()
    monkeypatch.setattr(
        profile_cmd.beam.io, "ReadFromText", mock_read_from_text
    )
    mock_create = mocker.Mock()
    monkeypatch.setattr(profile_cmd.beam, "Create", mock_create)

    mock_flatmap = mocker.Mock()
    monkeypatch.setattr(profile_cmd.beam, "FlatMap", mock_flatmap)

    mock_pardo = mocker.Mock()
    monkeypatch.setattr(profile_cmd.beam, "ParDo", mock_pardo)

    klio_pipeline._run_pipeline(transforms)

    mock_options.assert_called_once_with()
    assert 2 == mock_options.return_value.view_as.call_count

    if input_file:
        mock_read_from_text.assert_called_once_with(klio_pipeline.input_file)
        mock_pipeline.return_value.apply.assert_called_once_with(
            mock_read_from_text.return_value, mock_pipeline.return_value
        )
    else:
        mock_create.assert_called_once_with(klio_pipeline.entity_ids)
        mock_pipeline.return_value.apply.assert_called_once_with(
            mock_create.return_value, mock_pipeline.return_value
        )

    # file i/o / create
    assert 1 == mock_pipeline.return_value.apply.call_count

    # flatmap
    entity_ids = mock_pipeline.return_value.apply.return_value
    assert 1 == entity_ids.apply.call_count

    # pardo
    scaled_entity_ids = entity_ids.apply.return_value
    assert 3 == scaled_entity_ids.apply.call_count

    mock_pipeline.return_value.run.assert_called_once_with()
    mock_result = mock_pipeline.return_value.run
    mock_result.assert_called_once_with()


@pytest.mark.skip("TODO: fixme once we support profiling for v2")
def test_get_transforms(klio_pipeline, mocker, monkeypatch):
    transforms_module = mocker.Mock()

    transforms_module.AKlioClass = AKlioClass
    transforms_module.ANonKlioClass = ANonKlioClass

    mock_load_source = mocker.Mock()
    mock_load_source.return_value = transforms_module
    monkeypatch.setattr(profile_cmd.imp, "load_source", mock_load_source)

    transforms = list(klio_pipeline._get_transforms())

    mock_load_source.assert_called_once_with(
        "transforms", profile_cmd.KlioPipeline.TRANSFORMS_PATH
    )
    assert 1 == len(transforms)
    assert AKlioClass == transforms[0]


@pytest.mark.parametrize(
    "what", ("run", "cpu", "timeit", "memory", "memory_per_line", "foo")
)
def test_profile(what, klio_pipeline, mocker, monkeypatch):
    mock_get_profile_objects = mocker.Mock()
    monkeypatch.setattr(
        klio_pipeline, "_get_profile_objects", mock_get_profile_objects
    )

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

    mock_get_profile_objects.assert_called_once_with()

    if what == "run":
        mock_run_pipeline.assert_called_once_with(
            mock_get_profile_objects.return_value
        )
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
        mock_memory_per_line.assert_called_once_with(
            mock_get_profile_objects.return_value
        )
        mock_wall_time_per_line.assert_not_called()

    elif what == "timeit":
        mock_run_pipeline.assert_not_called()
        mock_profile_cpu.assert_not_called()
        mock_profile_memory.assert_not_called()
        mock_memory_per_line.assert_not_called()
        mock_wall_time_per_line.assert_called_once_with(
            mock_get_profile_objects.return_value
        )

    else:
        mock_run_pipeline.assert_not_called()
        mock_profile_cpu.assert_not_called()
        mock_profile_memory.assert_not_called()
        mock_memory_per_line.assert_not_called()
        mock_wall_time_per_line.assert_not_called()
