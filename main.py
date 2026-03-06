"""CLI entry point for autonomous pipeline operation."""

from __future__ import annotations

import argparse
import logging
import sys

from scraper.config import AutonomousConfig, ScheduleConfig
from scraper.health import get_health_report
from scraper.pipeline import SearchSpec
from scraper.scheduler import PipelineScheduler


def _parse_search_specs(raw_specs: list[str]) -> list[SearchSpec]:
    """Parse 'category:location' strings into SearchSpec objects."""
    specs: list[SearchSpec] = []
    for raw in raw_specs:
        if ":" not in raw:
            logging.error("Invalid search spec '%s'. Use 'category:location'", raw)
            sys.exit(1)
        category, location = raw.split(":", maxsplit=1)
        specs.append(SearchSpec(category=category.strip(), location=location.strip()))
    return specs


def cmd_run(args: argparse.Namespace) -> None:
    """Execute a single pipeline run."""
    specs = _parse_search_specs(args.search)
    config = AutonomousConfig.default()
    scheduler = PipelineScheduler(config, specs)
    scheduler.run_now()


def cmd_start(args: argparse.Namespace) -> None:
    """Start the scheduled pipeline loop."""
    specs = _parse_search_specs(args.search)
    config = AutonomousConfig.default()
    if args.interval:
        config = AutonomousConfig(
            schedule=ScheduleConfig(
                interval_hours=args.interval,
                run_on_start=True,
            ),
            pipeline=config.pipeline,
            retry=config.retry,
            state_file=config.state_file,
            logs_dir=config.logs_dir,
        )
    scheduler = PipelineScheduler(config, specs)
    scheduler.start()


def cmd_status(args: argparse.Namespace) -> None:
    """Show pipeline health status."""
    report = get_health_report()
    sys.stdout.write(f"Status:          {report.status}\n")
    sys.stdout.write(f"Last run:        {report.last_run_at or 'never'}\n")
    sys.stdout.write(f"Total runs:      {report.total_runs}\n")
    sys.stdout.write(f"Total discovered: {report.total_discovered}\n")
    sys.stdout.write(f"Total demos:     {report.total_demos}\n")
    sys.stdout.write(f"Error rate:      {report.error_rate:.1%}\n")


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Local Business Sites — Autonomous Pipeline",
    )
    subparsers = parser.add_subparsers(dest="command", required=True)

    run_parser = subparsers.add_parser("run", help="Execute a single pipeline run")
    run_parser.add_argument(
        "search",
        nargs="+",
        help="Search specs as 'category:location' (e.g., 'plumber:Denver, CO')",
    )
    run_parser.set_defaults(func=cmd_run)

    start_parser = subparsers.add_parser("start", help="Start scheduled pipeline loop")
    start_parser.add_argument(
        "search",
        nargs="+",
        help="Search specs as 'category:location'",
    )
    start_parser.add_argument(
        "--interval",
        type=float,
        default=None,
        help="Run interval in hours (default: 24)",
    )
    start_parser.set_defaults(func=cmd_start)

    status_parser = subparsers.add_parser("status", help="Show pipeline health")
    status_parser.set_defaults(func=cmd_status)

    args = parser.parse_args()
    logging.basicConfig(
        level=logging.INFO,
        format="[%(asctime)s] [%(levelname)s] %(message)s",
    )
    args.func(args)


if __name__ == "__main__":
    main()
