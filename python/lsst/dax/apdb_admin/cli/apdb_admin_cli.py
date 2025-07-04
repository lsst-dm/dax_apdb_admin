# This file is part of dax_apdb
#
# Developed for the LSST Data Management System.
# This product includes software developed by the LSST Project
# (https://www.lsst.org).
# See the COPYRIGHT file at the top-level directory of this distribution
# for details of code ownership.
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program.  If not, see <https://www.gnu.org/licenses/>.

from __future__ import annotations

__all__ = ["main"]

import argparse
from collections.abc import Sequence

from lsst.dax.apdb.cli.logging_cli import LoggingCli

from .. import scripts


def main(args: Sequence[str] | None = None) -> None:
    """Make apdb-admin command line tools.

    Parameters
    ----------
    args : `~collections.abc.Sequence` [`str`], optional
        Commad line arguments.
    """
    parser = argparse.ArgumentParser(description="APDB data management command line tools")
    log_cli = LoggingCli(parser)

    subparsers = parser.add_subparsers(title="available subcommands", required=True)
    _partition_subcommand(subparsers)
    _dump_subcommand(subparsers)
    _delete_subcommand(subparsers)

    parsed_args = parser.parse_args(args)
    log_cli.process_args(parsed_args)

    kwargs = vars(parsed_args)
    # Strip keywords not understood by scripts.
    method = kwargs.pop("method")
    method(**kwargs)


def _dump_subcommand(subparsers: argparse._SubParsersAction) -> None:
    parser = subparsers.add_parser("dump", help="Dump APDB contents.")
    subparsers = parser.add_subparsers(title="available subcommands", required=True)
    _dump_visit_subcommand(subparsers)


def _dump_visit_subcommand(subparsers: argparse._SubParsersAction) -> None:
    parser = subparsers.add_parser("visit", help="Dump data for particular visit.")
    parser.add_argument("butler_config", help="Butler configuration URI.")
    parser.add_argument("apdb_config", help="APDB configuration URI.")
    parser.add_argument("instrument", help="Instrument name.")
    parser.add_argument("visit", type=int, help="Visit number.")
    parser.add_argument("detectors", type=int, nargs="*", help="Detector number(s).")
    parser.add_argument(
        "-v", "--verbose", default=0, action="count", help="Verbose output, can use many times."
    )
    parser.set_defaults(method=scripts.dump_visit)


def _delete_subcommand(subparsers: argparse._SubParsersAction) -> None:
    parser = subparsers.add_parser("delete", help="Delete APDB contents.")
    subparsers = parser.add_subparsers(title="available subcommands", required=True)
    _delete_visit_subcommand(subparsers)


def _delete_visit_subcommand(subparsers: argparse._SubParsersAction) -> None:
    parser = subparsers.add_parser("visit", help="Delete DiaObjects first created in a particular visit.")
    parser.add_argument("butler_config", help="Butler configuration URI.")
    parser.add_argument("apdb_config", help="APDB configuration URI.")
    parser.add_argument("instrument", help="Instrument name.")
    parser.add_argument("visit", type=int, help="Visit number.")
    parser.add_argument("detectors", type=int, nargs="*", help="Detector number(s).")
    parser.add_argument(
        "--delete",
        default=False,
        action="store_true",
        help="Actually delete, by default only print records to be deleted.",
    )
    parser.add_argument(
        "--no-sources",
        default=False,
        action="store_true",
        help="Only selete objects that have no associated sources, and delete associated forced sources.",
    )
    parser.set_defaults(method=scripts.delete_visit)


def _partition_subcommand(subparsers: argparse._SubParsersAction) -> None:
    parser = subparsers.add_parser("partition", help="Partition information and management.")
    subparsers = parser.add_subparsers(title="available subcommands", required=True)
    _partition_show_region_subcommand(subparsers)
    _partition_show_period_subcommand(subparsers)
    _partition_show_time_part_subcommand(subparsers)


def _partition_show_region_subcommand(subparsers: argparse._SubParsersAction) -> None:
    parser = subparsers.add_parser("show-region", help="Show region corresponding to spatial partitions.")
    parser.add_argument("apdb_config", help="APDB configuration URI.")
    parser.add_argument("partitions", type=int, nargs="+", help="Partition number(s).")
    parser.set_defaults(method=scripts.partition_show_region)


def _partition_show_period_subcommand(subparsers: argparse._SubParsersAction) -> None:
    parser = subparsers.add_parser(
        "show-period", help="Show time period corresponding to temporal partitions."
    )
    parser.add_argument("apdb_config", help="APDB configuration URI.")
    parser.add_argument("partitions", type=int, nargs="+", help="Partition number(s).")
    parser.set_defaults(method=scripts.partition_show_period)


def _partition_show_time_part_subcommand(subparsers: argparse._SubParsersAction) -> None:
    parser = subparsers.add_parser(
        "show-time-part", help="Show time partition corresponding to timestamp(s)."
    )
    parser.add_argument("apdb_config", help="APDB configuration URI.")
    parser.add_argument(
        "timestamps",
        type=str,
        nargs="*",
        help="Timestamps in ISOT format and TAI scale. If not provided then current time is used.",
    )
    parser.add_argument(
        "-l", "--long", default=False, action="store_true", help="Show partition period for each entry."
    )
    parser.set_defaults(method=scripts.partition_show_time_part)
