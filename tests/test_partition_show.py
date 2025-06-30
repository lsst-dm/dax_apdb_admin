# This file is part of dax_apdb_admin.
#
# Developed for the LSST Data Management System.
# This product includes software developed by the LSST Project
# (http://www.lsst.org).
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
# along with this program.  If not, see <http://www.gnu.org/licenses/>.

from __future__ import annotations

import io
import tempfile
import unittest
from collections.abc import Iterator
from contextlib import contextmanager, redirect_stdout
from typing import Any

from lsst.dax.apdb.cassandra import ApdbCassandraConfig, ApdbCassandraPartitioningConfig
from lsst.dax.apdb_admin import scripts


class PartitionShowTestCase(unittest.TestCase):
    """A test case for 'partition show-*' CLI."""

    @contextmanager
    def make_config(self, **kwargs: Any) -> Iterator[str]:
        """Make ApdbCassandra configuration.

        Parameters
        ----------
        **kwargs
            Additional parameters to pass to pixelization configuration.
        """
        params = {
            "part_pixelization": "mq3c",
            "part_pix_level": 11,
            "query_per_spatial_part": False,
        }
        params.update(kwargs)

        partitioning = ApdbCassandraPartitioningConfig(**params)  # type: ignore[arg-type]
        config = ApdbCassandraConfig(partitioning=partitioning)
        with tempfile.NamedTemporaryFile("w") as tmpfile:
            config.save(tmpfile.name)
            yield tmpfile.name

    def test_show_period(self) -> None:
        """Test 'show period' subcommand."""
        with self.make_config() as config_path:
            with redirect_stdout(io.StringIO()) as stdout:
                scripts.partition_show_period(apdb_config=config_path, partitions=[674])
                output = stdout.getvalue()
                self.assertEqual(output, "674: start=2025-05-12T00:00:00.000 end=2025-06-11T00:00:00.000\n")

            with redirect_stdout(io.StringIO()) as stdout:
                scripts.partition_show_period(apdb_config=config_path, partitions=[720, 710, 700])
                output = stdout.getvalue()
                # Sorted in partition order.
                self.assertEqual(
                    output,
                    (
                        "700: start=2027-07-01T00:00:00.000 end=2027-07-31T00:00:00.000\n"
                        "710: start=2028-04-26T00:00:00.000 end=2028-05-26T00:00:00.000\n"
                        "720: start=2029-02-20T00:00:00.000 end=2029-03-22T00:00:00.000\n"
                    ),
                )

    def test_show_time_part(self) -> None:
        """Test 'show time-part' subcommand."""
        with self.make_config() as config_path:
            with redirect_stdout(io.StringIO()) as stdout:
                scripts.partition_show_time_part(
                    apdb_config=config_path, timestamps=["2025-01-01T00:00:00"], long=False
                )
                output = stdout.getvalue()
                self.assertEqual(output, "2025-01-01T00:00:00.000: 669\n")

            with redirect_stdout(io.StringIO()) as stdout:
                scripts.partition_show_time_part(
                    apdb_config=config_path, timestamps=["2025-01-01T00:00:00"], long=True
                )
                output = stdout.getvalue()
                self.assertEqual(
                    output,
                    "2025-01-01T00:00:00.000: 669 [2024-12-13T00:00:00.000, 2025-01-12T00:00:00.000)\n",
                )

            with redirect_stdout(io.StringIO()) as stdout:
                scripts.partition_show_time_part(
                    apdb_config=config_path,
                    timestamps=["2026-01-01T00:00:00", "2027-01-01T00:00:00", "2025-01-01T00:00:00"],
                    long=False,
                )
                output = stdout.getvalue()
                # Should be printed in time order.
                self.assertEqual(
                    output,
                    (
                        "2025-01-01T00:00:00.000: 669\n"
                        "2026-01-01T00:00:00.000: 681\n"
                        "2027-01-01T00:00:00.000: 693\n"
                    ),
                )

            with redirect_stdout(io.StringIO()) as stdout:
                scripts.partition_show_time_part(apdb_config=config_path, timestamps=[], long=False)
                output = stdout.getvalue()
                self.assertRegex(output, r"^[-\dT:.]+: \d+\n$")

    def test_show_region(self) -> None:
        """Test 'show region' subcommand."""
        with self.make_config() as config_path:
            with redirect_stdout(io.StringIO()) as stdout:
                scripts.partition_show_region(apdb_config=config_path, partitions=[62247462, 41953364])
                output = stdout.getvalue().strip().split("\n")
                self.assertEqual(len(output), 2)
                self.assertRegex(output[0], r"^41953364: lon=[-\d.]+° lat=[-\d.]+° region=ConvexPolygon.*$")
                self.assertRegex(output[1], r"^62247462: lon=[-\d.]+° lat=[-\d.]+° region=ConvexPolygon.*$")


if __name__ == "__main__":
    unittest.main()
