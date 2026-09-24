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

__all__ = ["dump_objects_with_no_sources"]

import itertools
import json
import logging
from typing import Any, NamedTuple, Protocol

import cassandra.concurrent

from lsst.dax.apdb import Apdb, ApdbTables
from lsst.dax.apdb.cassandra import ApdbCassandra
from lsst.dax.apdb.cassandra.queries import Column as C  # noqa: N817
from lsst.dax.apdb.cassandra.queries import Select
from lsst.sphgeom import Mq3cPixelization, log2

from .. import utils

_LOG = logging.getLogger(__name__)


class DiaObjectProtocol(Protocol):
    """Protocol for records in DiaObjectLAst table."""

    diaObjectId: int
    validityStartMjdTai: float
    ra: float
    dec: float
    nDiaSources: int

    def _asdict(self) -> dict[str, Any]: ...

    @property
    def _fields(self) -> tuple[str, ...]: ...


class DiaSourceProtocol(Protocol):
    """Protocol for records in regular DiaSource table."""

    apdb_part: int
    diaSourceId: int
    ra: float
    dec: float
    diaObjectId: int | None
    ssObjectId: int | None
    visit: int
    detector: int
    midpointMjdTai: float

    def _asdict(self) -> dict[str, Any]: ...

    @property
    def _fields(self) -> tuple[str, ...]: ...


class DiaObject(NamedTuple):
    """Subset of attributes of DiaObject records."""

    diaObjectId: int
    validityStartMjdTai: float
    ra: float
    dec: float
    nDiaSources: int

    @classmethod
    def from_row(cls, row: DiaObjectProtocol) -> DiaObject:
        return cls(
            diaObjectId=row.diaObjectId,
            validityStartMjdTai=row.validityStartMjdTai,
            ra=row.ra,
            dec=row.dec,
            nDiaSources=row.nDiaSources,
        )


class DiaSource(NamedTuple):
    """Subset of attributes of regular DiaSource records."""

    time_part: int
    apdb_part: int
    diaSourceId: int
    diaObjectId: int | None
    ssObjectId: int | None
    ra: float
    dec: float
    visit: int
    detector: int
    midpointMjdTai: float

    @classmethod
    def from_row(cls, time_part: int, row: DiaSourceProtocol) -> DiaSource:
        """Make SourceRecord from database-originated row.

        Parameters
        ----------
        time_part : `int`
            Time partition.
        row : `DiaSource`
            NamedTuple returned from database query.
        """
        return cls(
            time_part=time_part,
            apdb_part=row.apdb_part,
            diaSourceId=row.diaSourceId,
            diaObjectId=row.diaObjectId,
            ssObjectId=row.ssObjectId,
            ra=row.ra,
            dec=row.dec,
            visit=row.visit,
            detector=row.detector,
            midpointMjdTai=row.midpointMjdTai,
        )


def _make_apdb(apdb_config: str) -> ApdbCassandra:
    apdb = Apdb.from_uri(apdb_config)
    assert isinstance(apdb, ApdbCassandra), "Expecting Cassandra APDB"
    return apdb


def dump_objects_with_no_sources(
    apdb_config: str,
    pixel: str,
    num_pixels: int,
    jsonl: str | None,
) -> None:
    """Find and dump DiaObjects in the given region that have zero associated
    DiaSources.

    Parameters
    ----------
    apdb_config : `str`
        APDB configuration location.
    pixel : `str`
        Pixel specification, e.g. "MQ3C:3[640]".
    num_pixels : `int`
        Number of database pixels to process at once.
    jsonl : `str`, optional
        Name of the file to dump found DiaObjects to as JSON lines.
    """
    apdb = _make_apdb(apdb_config)

    context = apdb._context
    partitioner = context.partitioner
    db_pixelator = partitioner.pixelization.pixelator
    if not isinstance(db_pixelator, Mq3cPixelization):
        raise TypeError(f"Unsupported pixelization type {db_pixelator}")

    pixelization, pixel_id = utils.parse_pixel(pixel)
    pix_level = pixelization.level
    if not isinstance(pixelization.pixelator, Mq3cPixelization):
        raise TypeError(f"Argument pixelization {pixel} does not match database pixelization {db_pixelator}")
    db_pix_level = partitioner.pixelization.level
    if pix_level >= db_pix_level:
        raise ValueError(
            f"Pixelization level ({pixelization.level}) is finer than database level ({db_pix_level})"
        )

    # Pixelization level for intermediate pixels.
    mid_pix_level = db_pix_level - (log2(num_pixels) // 2)
    if pix_level > mid_pix_level:
        raise ValueError(
            f"Pixelization level ({pixelization.level}) is higher than intermediate level ({mid_pix_level})"
        )

    jout = open(jsonl, "w") if jsonl else None

    # How many bits to shift to go from large pixel to intermediate and to tiny
    shift = (mid_pix_level - pix_level) * 2
    shift2 = (db_pix_level - mid_pix_level) * 2

    object_count = 0
    source_count = 0
    no_source_count = 0
    for mid_pix_id in range(pixel_id << shift, (pixel_id + 1) << shift):
        # Database pixels to query in this iteration.
        db_pixels = list(range(mid_pix_id << shift2, (mid_pix_id + 1) << shift2))

        # We need to query somewhat wider area when looking for DiaSources,
        # just add all neighbor pixels.
        neighbor_pixels = sorted(
            set(itertools.chain.from_iterable(db_pixelator.neighborhood(db_pixel) for db_pixel in db_pixels))
        )

        _LOG.info(
            "Intermediate pixel ID: %s, found %d database pixels and %d neighbors",
            mid_pix_id,
            len(db_pixels),
            len(neighbor_pixels) - len(db_pixels),
        )

        dia_objects = _query_dia_objects(apdb, db_pixels)
        object_ids = {dia_object.diaObjectId for dia_object in dia_objects}
        dia_sources = _query_dia_sources(apdb, neighbor_pixels)
        source_object_ids = {dia_source.diaObjectId for dia_source in dia_sources}
        diff = object_ids - source_object_ids
        if diff:
            _LOG.warning("Found %d DiaObjects without DiaSources:", len(diff))
            for dia_object in dia_objects:
                if dia_object.diaObjectId in diff:
                    _LOG.warning("    %s", dia_object)

                    if jout:
                        json.dump(dia_object._asdict(), jout)
                        print("", file=jout)

        object_count += len(dia_objects)
        source_count += len(dia_sources)
        no_source_count += len(diff)

    _LOG.info(
        "Total %d objects, %d sources, %d objects without sources",
        object_count,
        source_count,
        no_source_count,
    )

    if jout:
        jout.close()


def _query_dia_objects(apdb: ApdbCassandra, db_pixels: list[int]) -> list[DiaObject]:
    context = apdb._context
    config = context.config

    table_name = context.schema.tableName(ApdbTables.DiaObjectLast)

    query = Select(config.keyspace, table_name, DiaObject._fields).where(C("apdb_part") == 0)
    statement = context.stmt_factory(query, prepare=True)

    queries = [(statement, (pixel,)) for pixel in db_pixels]

    results = cassandra.concurrent.execute_concurrent(
        context.session,
        queries,
        results_generator=True,
        raise_on_first_error=False,
        concurrency=config.connection_config.read_concurrency,
        execution_profile="read_named_tuples",
    )
    rows: list[DiaObject] = []
    for success, result in results:
        if success:
            rows += (DiaObject.from_row(row) for row in result)
        else:
            _LOG.error("error returned by query: %s", result)
            raise result

    _LOG.info("Found %d DiaObjects", len(rows))

    return rows


def _query_dia_sources(apdb: ApdbCassandra, db_pixels: list[int]) -> list[DiaSource]:
    context = apdb._context
    config = context.config

    if context.time_partitions_range is None:
        raise TypeError("DiaSource table must be time-partitioned")

    tables = {
        part: context.schema.tableName(ApdbTables.DiaSource, part)
        for part in range(context.time_partitions_range.start, context.time_partitions_range.end + 1)
    }
    columns = list(DiaSource._fields)
    columns.remove("time_part")

    rows: list[DiaSource] = []
    for part, table_name in tables.items():
        query = Select(config.keyspace, table_name, columns).where(C("apdb_part") == 0)
        statement = context.stmt_factory(query, prepare=True)

        queries = [(statement, (pixel,)) for pixel in db_pixels]

        results = cassandra.concurrent.execute_concurrent(
            context.session,
            queries,
            results_generator=True,
            raise_on_first_error=False,
            concurrency=config.connection_config.read_concurrency,
            execution_profile="read_named_tuples",
        )
        for success, result in results:
            if success:
                rows += (DiaSource.from_row(part, row) for row in result)
            else:
                _LOG.error("error returned by query: %s", result)
                raise result

    _LOG.info("Found %d DiaSources", len(rows))

    return rows
