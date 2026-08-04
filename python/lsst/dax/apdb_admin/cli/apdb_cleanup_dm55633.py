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

from ..scripts import cleanup_dm55633


def main(args: Sequence[str] | None = None) -> None:
    """Cleanup duplicate DiaSources (DM-55633).

    Parameters
    ----------
    args : `~collections.abc.Sequence` [`str`], optional
        Command line arguments.
    """
    parser = argparse.ArgumentParser(description="Cleanup duplicate DiaSources")
    log_cli = LoggingCli(parser)

    subparsers = parser.add_subparsers(title="available subcommands", required=True)
    _find_subcommand(subparsers)
    _sources_to_delete_subcommand(subparsers)
    _sources_to_keep_subcommand(subparsers)
    _find_sources_subcommand(subparsers)
    _find_replica_objects_subcommand(subparsers)

    parsed_args = parser.parse_args(args)
    log_cli.process_args(parsed_args)

    kwargs = vars(parsed_args)
    # Strip keywords not understood by scripts.
    method = kwargs.pop("method")
    method(**kwargs)


def _find_subcommand(subparsers: argparse._SubParsersAction) -> None:
    parser = subparsers.add_parser("find", help="Find visit/detector pairs processed multiple times.")
    parser.add_argument("apdb_config", help="APDB configuration URI.")
    parser.set_defaults(method=cleanup_dm55633.find_visit_detector)


def _sources_to_delete_subcommand(subparsers: argparse._SubParsersAction) -> None:
    parser = subparsers.add_parser("sources-to-delete", help="Find DiaSources to delete.")
    parser.add_argument("apdb_config", help="APDB configuration URI.")
    parser.add_argument("visit_detector", help="Path to CSV file produced by `find`.")
    parser.set_defaults(method=cleanup_dm55633.sources_to_delete)


def _sources_to_keep_subcommand(subparsers: argparse._SubParsersAction) -> None:
    parser = subparsers.add_parser("sources-to-keep", help="Find DiaSources to keep.")
    parser.add_argument("apdb_config", help="APDB configuration URI.")
    parser.add_argument("visit_detector", help="Path to CSV file produced by `find`.")
    parser.set_defaults(method=cleanup_dm55633.sources_to_keep)


def _find_sources_subcommand(subparsers: argparse._SubParsersAction) -> None:
    parser = subparsers.add_parser("find-sources", help="Find matching DiaSources in regular tables.")
    parser.add_argument("csv_file", help="Path to CSV file produced by `sources-to-delete/keep`.")
    parser.add_argument("butler_config", help="Butler configuration URI.")
    parser.add_argument("apdb_config", help="APDB configuration URI.")
    parser.set_defaults(method=cleanup_dm55633.find_matching_sources)


def _find_replica_objects_subcommand(subparsers: argparse._SubParsersAction) -> None:
    parser = subparsers.add_parser("find-replica-objects", help="Find matching DiaObjects in replica tables.")
    parser.add_argument("csv_file", help="Path to CSV file produced by `sources-to-delete/keep`.")
    parser.add_argument("apdb_config", help="APDB configuration URI.")
    parser.set_defaults(method=cleanup_dm55633.find_replica_objects)
