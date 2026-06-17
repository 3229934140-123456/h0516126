from __future__ import annotations

import os
import sys


def run_quick_example():
    sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

    from cicd_engine import PipelineEngine
    from cicd_engine.logging.log_stream import LogLevel
    from cicd_engine.scheduler.scheduler import SchedulerEventType

    print("=" * 70)
    print("CI/CD Pipeline Engine - Quick Start Example")
    print("=" * 70)

    example_pipeline = {
        "name": "Quick Start Pipeline",
        "version": "1.0",
        "description": "A simple demonstration pipeline",
        "variables": [
            {"key": "PROJECT", "value": "demo"},
            {"key": "VERSION", "value": "2.0.0"},
        ],
        "stages": [
            {
                "name": "init",
                "steps": [
                    {
                        "name": "hello",
                        "script": [
                            "echo Hello from CI/CD Engine!",
                            "echo Project: $PROJECT",
                            "echo Version: $VERSION",
                            "echo ::set-output name=status::ready",
                        ],
                    }
                ],
            },
            {
                "name": "work",
                "depends_on": ["init"],
                "parallel": True,
                "steps": [
                    {
                        "name": "task_a",
                        "script": [
                            "echo Running Task A...",
                            "echo Task A completed",
                            "echo ::set-output name=task_a::done",
                        ],
                    },
                    {
                        "name": "task_b",
                        "script": [
                            "echo Running Task B...",
                            "echo Task B completed",
                            "echo ::set-output name=task_b::done",
                        ],
                    },
                ],
            },
            {
                "name": "final",
                "depends_on": ["work"],
                "steps": [
                    {
                        "name": "summary",
                        "script": [
                            "echo Pipeline complete!",
                            "echo All tasks finished successfully",
                        ],
                    }
                ],
            },
        ],
    }

    with PipelineEngine.in_memory(max_workers=4, console_output=True,
                                   min_console_level=LogLevel.DEBUG) as engine:
        print("\n--- Validating pipeline definition ---")
        valid, errors, warnings = engine.validate(example_pipeline)
        if valid:
            print("Pipeline is VALID!")
            if warnings:
                for w in warnings:
                    print(f"  Warning: {w}")
        else:
            print("Pipeline is INVALID!")
            for e in errors:
                print(f"  Error: {e}")
            return 1

        print("\n--- Dry Run (Execution Plan) ---")
        plan = engine.dry_run(example_pipeline)
        print(f"Total stages: {plan['total_stages']}")
        print(f"Total steps: {plan['total_steps']}")
        for idx, group in enumerate(plan['parallel_groups']):
            print(f"  Parallel batch {idx + 1}: {', '.join(group)}")

        print("\n--- Executing pipeline ---")

        def on_event(event):
            if event.type in (SchedulerEventType.PIPELINE_COMPLETED,
                              SchedulerEventType.PIPELINE_FAILED,
                              SchedulerEventType.STEP_COMPLETED,
                              SchedulerEventType.STEP_FAILED):
                pass

        engine.on_event(on_event)

        result = engine.run(example_pipeline)

        print("\n--- Pipeline Result ---")
        print(f"Status: {result.status.value.upper()}")
        print(f"Duration: {result.duration:.2f}s" if result.duration else "Duration: N/A")
        print(f"Total steps: {result.total_steps}")
        print(f"  Success: {result.success_count}")
        print(f"  Failed: {result.failed_count}")
        print(f"  Skipped: {result.skipped_count}")
        print()

        for stage_result in result.stage_results:
            icon = "✓" if stage_result.status.value in ("success", "skipped") else "✗"
            dur = f"{stage_result.duration:.2f}s" if stage_result.duration else "N/A"
            print(f"  {icon} Stage '{stage_result.stage_name}' [{dur}]")
            for step_result in stage_result.step_results:
                step_icon = "✓" if step_result.status.value in ("success", "skipped") else "✗"
                step_dur = f"{step_result.duration:.2f}s" if step_result.duration else "N/A"
                extra = ""
                if step_result.error_message:
                    extra = f" - {step_result.error_message}"
                print(f"      {step_icon} Step '{step_result.step_name}' [{step_dur}] "
                      f"({step_result.status.value}){extra}")

        return 0 if result.status.value in ("success", "partial_success") else 1


if __name__ == "__main__":
    try:
        exit(run_quick_example())
    except KeyboardInterrupt:
        print("\nInterrupted by user")
        exit(130)
