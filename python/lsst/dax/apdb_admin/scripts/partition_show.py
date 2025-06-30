# This file is part of dax_apdb_admin
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

__all__ = ["partition_show_region"]

import logging
from collections.abc import Collection

import astropy.time

from lsst.dax.apdb import ApdbConfig
from lsst.dax.apdb.cassandra import ApdbCassandraConfig
from lsst.dax.apdb.cassandra.partitioner import Partitioner
from lsst.dax.apdb.pixelization import Pixelization
from lsst.dax.apdb.sql import ApdbSqlConfig

_LOG = logging.getLogger(__name__)


def partition_show_region(apdb_config: str, partitions: Collection[int]) -> None:
    """Show regions corresponding to partitions.

    Parameters
    ----------
    apdb_config : `str`
        APDB configuration location.
    partitions : `~collections.abc.Collection` [`int`]
        List of partitions.
    """
    # No need to instantiate Apdb, we can look at config.
    config = ApdbConfig.from_uri(apdb_config)
    if isinstance(config, ApdbCassandraConfig):
        partitioner = Partitioner(config)
        pixelization = partitioner.pixelization
    elif isinstance(config, ApdbSqlConfig):
        pixelization = Pixelization("htm", config.pixelization.htm_level, 100)
    else:
        raise TypeError("Unsupported type of APDB configuration.")

    for partition in sorted(partitions):
        try:
            region = pixelization.region(partition)
        except ValueError:
            print(f"Invalid pixel index: {partition}")
            continue
        center = region.getBoundingBox().getCenter()
        lon = center.getLon().asDegrees()
        lat = center.getLat().asDegrees()
        print(f"{partition}: lon={lon:.2f}° lat={lat:.2f}° region={region}")


def partition_show_period(apdb_config: str, partitions: Collection[int]) -> None:
    """Show time periods corresponding to partitions.

    Parameters
    ----------
    apdb_config : `str`
        APDB configuration location.
    partitions : `~collections.abc.Collection` [`int`]
        List of partitions.
    """
    # No need to instantiate Apdb, we can look at config.
    config = ApdbConfig.from_uri(apdb_config)
    if isinstance(config, ApdbCassandraConfig):
        partitioner = Partitioner(config)
        for partition in sorted(partitions):
            start, end = partitioner.partition_period(partition)
            print(f"{partition}: start={start.isot} end={end.isot}")
    else:
        raise TypeError("Unsupported type of APDB configuration.")


def partition_show_time_part(apdb_config: str, timestamps: Collection[str], long: bool) -> None:
    """Show time partition corresponding to timestamps or to current time
    if timestamps are not provided.

    Parameters
    ----------
    apdb_config : `str`
        APDB configuration location.
    timestamps : `~collections.abc.Collection` [`str`]
        List of timestamps, in "isot" format and TAI scale.
    long : `bool`
        If Trtue also show period for each entry.
    """
    ts = sorted(astropy.time.Time(timestamp_str, format="isot", scale="tai") for timestamp_str in timestamps)
    if not ts:
        ts = [astropy.time.Time.now().tai]

    config = ApdbConfig.from_uri(apdb_config)
    if isinstance(config, ApdbCassandraConfig):
        partitioner = Partitioner(config)
        for timestamp in ts:
            partition = partitioner.time_partition(timestamp)
            if long:
                start, end = partitioner.partition_period(partition)
                print(f"{timestamp.isot}: {partition} [{start.isot}, {end.isot})")
            else:
                print(f"{timestamp.isot}: {partition}")
    else:
        raise TypeError("Unsupported type of APDB configuration.")
