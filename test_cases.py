from __future__ import annotations

import os
import sys
import time

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from cicd_engine import PipelineEngine
from cicd_engine.logging.log_stream import LogLevel
from cicd_engine.models.pipeline import FailureStrategy, PipelineStatus, StepStatus


def test_basic_success():
    """1. 基础成功场景（回归测试）"""
    print("\n" + "=" * 60)
    print("TEST 1: Basic Success Pipeline (Regression)")
    print("=" * 60)

    pipeline = {
        "name": "basic_test",
        "stages": [
            {
                "name": "build",
                "steps": [
                    {
                        "name": "echo",
                        "script": ["echo build ok"],
                    }
                ],
            },
            {
                "name": "test",
                "depends_on": ["build"],
                "steps": [
                    {
                        "name": "echo",
                        "script": ["echo test ok"],
                    }
                ],
            },
        ],
    }

    with PipelineEngine.in_memory(max_workers=4, min_console_level=LogLevel.WARN, console_output=False) as engine:
        result = engine.run(pipeline)

    assert result.status == PipelineStatus.SUCCESS
    assert result.success_count == 2
    assert result.failed_count == 0
    assert result.duration is not None and result.duration >= 0
    print(f"  ✓ Pipeline succeeded in {result.duration:.2f}s")
    return True


def test_failure_strategy_continue():
    """2. 失败后继续：无依赖分支继续跑，失败分支后续被跳过"""
    print("\n" + "=" * 60)
    print("TEST 2: Failure Strategy = CONTINUE")
    print("=" * 60)

    pipeline = {
        "name": "failure_continue_test",
        "failure_strategy": "continue",
        "stages": [
            {
                "name": "main_build",
                "steps": [
                    {
                        "name": "succeed_step",
                        "script": ["echo main build"],
                    },
                    {
                        "name": "fail_step",
                        "script": ["exit 1"],
                    },
                ],
            },
            {
                "name": "independent",
                "depends_on": [],
                "failure_strategy": "continue",
                "steps": [
                    {
                        "name": "independent_step",
                        "script": ["echo independent ok"],
                    }
                ],
            },
        ],
    }

    with PipelineEngine.in_memory(max_workers=4, min_console_level=LogLevel.WARN, console_output=False) as engine:
        result = engine.run(pipeline)
        state = engine.get_pipeline_state(result.pipeline_id)

    print(f"  Status: {result.status.value}")

    stage_results = {sr.stage_name: sr for sr in result.stage_results}

    main_build = stage_results["main_build"]
    independent = stage_results["independent"]

    main_succeed = None
    main_fail = None
    for sr in main_build.step_results:
        if sr.step_name == "succeed_step":
            main_succeed = sr
        elif sr.step_name == "fail_step":
            main_fail = sr

    assert main_succeed is not None and main_succeed.status == StepStatus.SUCCESS
    print(f"  ✓ main_build/succeed_step: {main_succeed.status.value}")

    assert main_fail is not None and main_fail.status == StepStatus.FAILED
    print(f"  ✓ main_build/fail_step: {main_fail.status.value}")

    assert independent.step_results[0].status == StepStatus.SUCCESS
    print(f"  ✓ independent/independent_step: {independent.step_results[0].status.value}")

    assert result.status in (PipelineStatus.PARTIAL_SUCCESS, PipelineStatus.SUCCESS)
    print(f"  ✓ Overall: {result.status.value}")

    return True


def test_upstream_failure_skip_dependents():
    """3. 上游失败，依赖它的步骤被正确跳过，且有错误信息"""
    print("\n" + "=" * 60)
    print("TEST 3: Upstream Failure Skips Dependents")
    print("=" * 60)

    pipeline = {
        "name": "upstream_skip_test",
        "failure_strategy": "continue",
        "stages": [
            {
                "name": "stage_a",
                "steps": [
                    {
                        "name": "will_fail",
                        "script": ["exit 1"],
                    },
                    {
                        "name": "depends_on_fail",
                        "depends_on": ["will_fail"],
                        "script": ["echo should not run"],
                    },
                ],
            },
            {
                "name": "stage_b",
                "depends_on": ["stage_a"],
                "failure_strategy": "continue",
                "steps": [
                    {
                        "name": "step_b1",
                        "script": ["echo stage b ok"],
                    }
                ],
            },
            {
                "name": "stage_c",
                "depends_on": [],
                "steps": [
                    {
                        "name": "step_c1",
                        "depends_on": [],
                        "script": ["echo stage c ok"],
                    }
                ],
            },
        ],
    }

    with PipelineEngine.in_memory(max_workers=4, min_console_level=LogLevel.WARN, console_output=False) as engine:
        result = engine.run(pipeline)

    print(f"  Status: {result.status.value}")

    step_map = {}
    for sr in result.stage_results:
        for step_r in sr.step_results:
            step_map[f"{sr.stage_name}/{step_r.step_name}"] = step_r

    fail = step_map["stage_a/will_fail"]
    skip = step_map["stage_a/depends_on_fail"]
    step_b1 = step_map["stage_b/step_b1"]
    step_c1 = step_map["stage_c/step_c1"]

    assert fail.status == StepStatus.FAILED
    print(f"  ✓ stage_a/will_fail: {fail.status.value}")

    assert skip.status == StepStatus.SKIPPED
    assert skip.error_message and "upstream failure" in skip.error_message
    print(f"  ✓ stage_a/depends_on_fail: SKIPPED ({skip.error_message})")

    assert step_c1.status == StepStatus.SUCCESS
    print(f"  ✓ stage_c/step_c1: {step_c1.status.value}")

    assert step_b1.status == StepStatus.SKIPPED
    assert step_b1.error_message and "upstream failure" in step_b1.error_message
    print(f"  ✓ stage_b/step_b1: SKIPPED (depends on failed stage_a)")

    print("  ✓ Only truly independent stage and its step was skipped correctly!")
    return True


def test_timeout_termination():
    """4. 命令超时后该步骤被终止，状态标记为 TIMEOUT，不影响其他步骤"""
    print("\n" + "=" * 60)
    print("TEST 4: Timeout Termination")
    print("=" * 60)

    sleep_cmd = "timeout /t 30 2>nul || sleep 30" if sys.platform == "win32" else "sleep 30"

    pipeline = {
        "name": "timeout_test",
        "failure_strategy": "continue",
        "stages": [
            {
                "name": "slow_stage",
                "steps": [
                    {
                        "name": "slow_step",
                        "script": [sleep_cmd],
                        "timeout": 2,
                    },
                    {
                        "name": "after_timeout",
                        "depends_on": ["slow_step"],
                        "script": ["echo should be skipped"],
                    },
                ],
            },
            {
                "name": "fast_stage",
                "depends_on": [],
                "steps": [
                    {
                        "name": "fast_step",
                        "script": ["echo fast ok"],
                    }
                ],
            },
        ],
    }

    with PipelineEngine.in_memory(max_workers=4, min_console_level=LogLevel.WARN, console_output=False) as engine:
        start = time.time()
        result = engine.run(pipeline)
        elapsed = time.time() - start

    print(f"  Elapsed: {elapsed:.2f}s (should be ~2s, not 30s)")
    assert elapsed < 10, "Timeout did not terminate quickly!"
    print(f"  ✓ Pipeline completed in {elapsed:.2f}s (timeout worked)")

    step_map = {}
    for sr in result.stage_results:
        for step_r in sr.step_results:
            step_map[f"{sr.stage_name}/{step_r.step_name}"] = step_r

    slow = step_map["slow_stage/slow_step"]
    after = step_map["slow_stage/after_timeout"]
    fast = step_map["fast_stage/fast_step"]

    assert slow.status == StepStatus.TIMEOUT
    print(f"  ✓ slow_step status: {slow.status.value}")

    assert after.status == StepStatus.SKIPPED
    print(f"  ✓ after_timeout status: {after.status.value}")

    assert fast.status == StepStatus.SUCCESS
    print(f"  ✓ fast_step status: {fast.status.value} (ran independently)")

    return True


def test_state_persistence_and_reload():
    """5. 执行结束后状态完整保存，重新读取不丢失任何信息"""
    print("\n" + "=" * 60)
    print("TEST 5: State Persistence and Reload")
    print("=" * 60)

    import tempfile
    import shutil

    base_dir = tempfile.mkdtemp(prefix="cicd_test_state_")
    print(f"  Using temp dir: {base_dir}")

    engine1 = PipelineEngine(base_dir=base_dir, console_output=False, min_console_level=LogLevel.WARN)

    pipeline = {
        "name": "persistence_test",
        "stages": [
            {
                "name": "s1",
                "steps": [
                    {"name": "step1", "script": ["echo hello"]},
                    {"name": "step2", "script": ["exit 1"]},
                ],
            }
        ],
    }

    try:
        result1 = engine1.run(pipeline)
        pid = result1.pipeline_id

        print(f"  Original result duration: {result1.duration}s")
        print(f"  Original status: {result1.status.value}")
        print(f"  Original end_time set: {result1.end_time is not None}")
        print(f"  Original start_time set: {result1.start_time is not None}")

        for sr in result1.stage_results:
            for step_r in sr.step_results:
                print(f"    {sr.stage_name}/{step_r.step_name}: {step_r.status.value}, "
                      f"duration={step_r.duration}s")

        engine1.shutdown()
        del engine1

        engine2 = PipelineEngine(base_dir=base_dir, console_output=False, min_console_level=LogLevel.WARN)

        state2 = engine2.get_pipeline_state(pid)
        result2 = engine2.get_pipeline_result(pid)

        print(f"\n  Reloaded status: {result2.status.value}")
        print(f"  Reloaded duration: {result2.duration}s")
        print(f"  Reloaded start_time set: {result2.start_time is not None}")
        print(f"  Reloaded end_time set: {result2.end_time is not None}")

        assert result2.start_time is not None, "start_time lost after reload!"
        assert result2.end_time is not None, "end_time lost after reload!"
        assert result2.duration is not None and result2.duration >= 0, "duration lost after reload!"
        print("  ✓ start_time, end_time, duration all preserved!")

        for sr in result2.stage_results:
            for step_r in sr.step_results:
                assert step_r.start_time is not None, f"{step_r.step_name} start_time lost!"
                assert step_r.end_time is not None, f"{step_r.step_name} end_time lost!"
                assert step_r.duration is not None, f"{step_r.step_name} duration lost!"
                print(f"  ✓ {sr.stage_name}/{step_r.step_name}: start={step_r.start_time is not None}, "
                      f"end={step_r.end_time is not None}, dur={step_r.duration}s")

        assert result2.status == result1.status
        print(f"  ✓ Status preserved: {result2.status.value}")

        engine2.shutdown()

    finally:
        try:
            shutil.rmtree(base_dir)
        except Exception:
            pass

    return True


def test_allow_failure():
    """6. allow_failure: 步骤失败但不影响后续"""
    print("\n" + "=" * 60)
    print("TEST 6: allow_failure Step")
    print("=" * 60)

    pipeline = {
        "name": "allow_fail_test",
        "failure_strategy": "abort",
        "stages": [
            {
                "name": "stage1",
                "steps": [
                    {
                        "name": "fail_allowed",
                        "script": ["exit 1"],
                        "allow_failure": True,
                    },
                    {
                        "name": "should_run",
                        "depends_on": ["fail_allowed"],
                        "script": ["echo runs anyway"],
                    },
                ],
            }
        ],
    }

    with PipelineEngine.in_memory(max_workers=4, min_console_level=LogLevel.WARN, console_output=False) as engine:
        result = engine.run(pipeline)

    step_map = {}
    for sr in result.stage_results:
        for step_r in sr.step_results:
            step_map[step_r.step_name] = step_r

    fail_step = step_map["fail_allowed"]
    next_step = step_map["should_run"]

    assert fail_step.status == StepStatus.SUCCESS
    print(f"  ✓ fail_allowed: {fail_step.status.value} (allow_failure=true)")

    assert next_step.status == StepStatus.SUCCESS
    print(f"  ✓ should_run: {next_step.status.value} (depends on allow_failure step)")

    assert result.status == PipelineStatus.SUCCESS
    print(f"  ✓ Pipeline: {result.status.value}")
    return True


def test_condition_skip():
    """7. 条件不满足时步骤被跳过"""
    print("\n" + "=" * 60)
    print("TEST 7: Condition-based Skip")
    print("=" * 60)

    pipeline = {
        "name": "condition_test",
        "variables": [{"key": "DEPLOY", "value": "false"}],
        "stages": [
            {
                "name": "build",
                "steps": [{"name": "compile", "script": ["echo build"]}],
            },
            {
                "name": "deploy",
                "depends_on": ["build"],
                "condition": {"variable": "DEPLOY", "operator": "eq", "value": "true"},
                "steps": [{"name": "deploy_step", "script": ["echo deploy"]}],
            },
        ],
    }

    with PipelineEngine.in_memory(max_workers=4, min_console_level=LogLevel.WARN, console_output=False) as engine:
        result = engine.run(pipeline)

    stage_map = {sr.stage_name: sr for sr in result.stage_results}

    build = stage_map["build"]
    deploy = stage_map["deploy"]

    assert build.status == StepStatus.SUCCESS
    print(f"  ✓ build stage: {build.status.value}")

    assert deploy.status == StepStatus.SKIPPED
    print(f"  ✓ deploy stage: {deploy.status.value} (condition false)")
    assert deploy.step_results[0].status == StepStatus.SKIPPED
    print(f"  ✓ deploy step: {deploy.step_results[0].status.value}")

    return True


def test_stage_mixed_result_skips_downstream():
    """8. CONTINUE 策略：阶段内有成功和失败步骤时，阶段本身是失败，下游阶段被跳过"""
    print("\n" + "=" * 60)
    print("TEST 8: Stage Mixed Result Skips Downstream")
    print("=" * 60)

    pipeline = {
        "name": "mixed_stage_test",
        "failure_strategy": "continue",
        "stages": [
            {
                "name": "build",
                "steps": [
                    {"name": "compile", "script": ["echo compile ok"]},
                    {"name": "lint", "script": ["exit 1"]},
                    {"name": "test", "depends_on": ["compile"], "script": ["echo test ok"]},
                ],
            },
            {
                "name": "deploy",
                "depends_on": ["build"],
                "steps": [
                    {"name": "deploy_step", "script": ["echo deploy should not run"]},
                ],
            },
            {
                "name": "notify",
                "depends_on": [],
                "steps": [
                    {"name": "send_notification", "script": ["echo notification ok"]},
                ],
            },
        ],
    }

    with PipelineEngine.in_memory(max_workers=4, min_console_level=LogLevel.WARN, console_output=False) as engine:
        result = engine.run(pipeline)

    print(f"  Overall status: {result.status.value}")

    stage_map = {sr.stage_name: sr for sr in result.stage_results}

    build = stage_map["build"]
    deploy = stage_map["deploy"]
    notify = stage_map["notify"]

    step_map = {}
    for sr in result.stage_results:
        for step_r in sr.step_results:
            step_map[f"{sr.stage_name}/{step_r.step_name}"] = step_r

    assert step_map["build/compile"].status == StepStatus.SUCCESS
    print(f"  ✓ build/compile: {step_map['build/compile'].status.value}")

    assert step_map["build/lint"].status == StepStatus.FAILED
    print(f"  ✓ build/lint: {step_map['build/lint'].status.value}")

    assert step_map["build/test"].status == StepStatus.SUCCESS
    print(f"  ✓ build/test: {step_map['build/test'].status.value} (parallel, no dep on lint)")

    assert build.status == StepStatus.FAILED
    print(f"  ✓ build stage: {build.status.value} (has failed step, so stage is failed)")

    assert deploy.status == StepStatus.SKIPPED
    print(f"  ✓ deploy stage: {deploy.status.value} (depends on failed build)")
    assert step_map["deploy/deploy_step"].status == StepStatus.SKIPPED
    print(f"  ✓ deploy/deploy_step: {step_map['deploy/deploy_step'].status.value}")

    assert notify.status == StepStatus.SUCCESS
    print(f"  ✓ notify stage: {notify.status.value} (independent, continues)")

    assert result.status in (PipelineStatus.PARTIAL_SUCCESS, PipelineStatus.FAILED)
    print(f"  ✓ Pipeline overall: {result.status.value}")

    return True


def test_abort_status_reload_times():
    """9. ABORT 策略失败中止后，重读状态，所有跳过/取消的节点都有结束时间和耗时"""
    print("\n" + "=" * 60)
    print("TEST 9: ABORT Strategy Status Reload - All Times Present")
    print("=" * 60)

    import tempfile
    import shutil

    base_dir = tempfile.mkdtemp(prefix="cicd_test_abort_")
    print(f"  Using temp dir: {base_dir}")

    pipeline = {
        "name": "abort_time_test",
        "failure_strategy": "abort",
        "stages": [
            {
                "name": "stage1",
                "steps": [
                    {"name": "step_a", "script": ["echo step a ok"]},
                    {"name": "step_b", "script": ["exit 1"]},
                    {"name": "step_c", "depends_on": ["step_a"], "script": ["echo step c"]},
                ],
            },
            {
                "name": "stage2",
                "depends_on": ["stage1"],
                "steps": [
                    {"name": "step_d", "script": ["echo step d should not run"]},
                    {"name": "step_e", "script": ["echo step e should not run"]},
                ],
            },
            {
                "name": "stage3",
                "depends_on": ["stage2"],
                "steps": [
                    {"name": "step_f", "script": ["echo step f should not run"]},
                ],
            },
        ],
    }

    try:
        engine1 = PipelineEngine(base_dir=base_dir, console_output=False, min_console_level=LogLevel.WARN)
        result1 = engine1.run(pipeline)
        pid = result1.pipeline_id

        print(f"  Original status: {result1.status.value}")
        print(f"  Original duration: {result1.duration}s")
        print(f"  Original start_time set: {result1.start_time is not None}")
        print(f"  Original end_time set: {result1.end_time is not None}")

        for sr in result1.stage_results:
            for step_r in sr.step_results:
                print(f"    {sr.stage_name}/{step_r.step_name}: {step_r.status.value}, "
                      f"start={step_r.start_time is not None}, end={step_r.end_time is not None}, "
                      f"dur={step_r.duration}s")

        for sr in result1.stage_results:
            print(f"    Stage {sr.stage_name}: {sr.status.value}, "
                  f"start={sr.start_time is not None}, end={sr.end_time is not None}, "
                  f"dur={sr.duration}s")

        all_have_times = True
        for sr in result1.stage_results:
            if sr.start_time is None or sr.end_time is None or sr.duration is None:
                all_have_times = False
                print(f"  ✗ Stage {sr.stage_name} missing times")
            for step_r in sr.step_results:
                if step_r.start_time is None or step_r.end_time is None or step_r.duration is None:
                    all_have_times = False
                    print(f"  ✗ Step {sr.stage_name}/{step_r.step_name} missing times")

        assert all_have_times, "Some steps/stages missing start_time/end_time/duration!"
        print("  ✓ All steps and stages have start_time, end_time, duration (before reload)")

        engine1.shutdown()
        del engine1

        engine2 = PipelineEngine(base_dir=base_dir, console_output=False, min_console_level=LogLevel.WARN)

        result2 = engine2.get_pipeline_result(pid)

        print(f"\n  Reloaded status: {result2.status.value}")
        print(f"  Reloaded duration: {result2.duration}s")

        all_have_times_after_reload = True
        for sr in result2.stage_results:
            if sr.start_time is None or sr.end_time is None or sr.duration is None:
                all_have_times_after_reload = False
                print(f"  ✗ Stage {sr.stage_name} missing times after reload")
            for step_r in sr.step_results:
                if step_r.start_time is None or step_r.end_time is None or step_r.duration is None:
                    all_have_times_after_reload = False
                    print(f"  ✗ Step {sr.stage_name}/{step_r.step_name} missing times after reload")
                else:
                    print(f"  ✓ {sr.stage_name}/{step_r.step_name}: {step_r.status.value}, dur={step_r.duration:.4f}s")

        assert all_have_times_after_reload, "Some steps/stages missing times after reload!"
        print("  ✓ All steps and stages have times after reload!")

        skipped_steps = [
            step_r for sr in result2.stage_results
            for step_r in sr.step_results
            if step_r.status == StepStatus.SKIPPED
        ]
        cancelled_stages = [
            sr for sr in result2.stage_results
            if sr.status == StepStatus.CANCELLED
        ]

        print(f"  ✓ Found {len(skipped_steps)} skipped steps, all with duration")
        print(f"  ✓ Found {len(cancelled_stages)} cancelled stages, all with duration")

        assert result2.start_time is not None
        assert result2.end_time is not None
        assert result2.duration is not None
        print("  ✓ Pipeline-level start/end/duration all present after reload")

        engine2.shutdown()

    finally:
        try:
            shutil.rmtree(base_dir)
        except Exception:
            pass

    return True


def run_all_tests():
    tests = [
        ("Basic Success", test_basic_success),
        ("Failure Strategy CONTINUE", test_failure_strategy_continue),
        ("Upstream Failure Skips Dependents", test_upstream_failure_skip_dependents),
        ("Timeout Termination", test_timeout_termination),
        ("State Persistence", test_state_persistence_and_reload),
        ("allow_failure", test_allow_failure),
        ("Condition Skip", test_condition_skip),
        ("Stage Mixed Result Skips Downstream", test_stage_mixed_result_skips_downstream),
        ("ABORT Status Reload Times", test_abort_status_reload_times),
    ]

    passed = 0
    failed = 0

    for name, test_fn in tests:
        try:
            if test_fn():
                passed += 1
                print(f"  ✓ Test PASSED")
            else:
                failed += 1
                print(f"  ✗ Test FAILED (returned False)")
        except AssertionError as e:
            failed += 1
            print(f"  ✗ Test FAILED: {e}")
            import traceback
            traceback.print_exc()
        except Exception as e:
            failed += 1
            print(f"  ✗ Test ERROR: {e}")
            import traceback
            traceback.print_exc()

    print("\n" + "=" * 60)
    print(f"Summary: {passed}/{len(tests)} passed, {failed} failed")
    print("=" * 60)

    if failed:
        sys.exit(1)


if __name__ == "__main__":
    run_all_tests()
