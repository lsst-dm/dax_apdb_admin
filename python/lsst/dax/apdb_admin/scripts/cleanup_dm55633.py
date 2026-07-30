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

__all__ = ["find_duplicates"]

import csv
import logging
import sys
from collections import Counter, defaultdict
from collections.abc import Iterator
from typing import NamedTuple, cast

import cassandra.concurrent
from astropy.time import Time

from lsst.daf.butler import Butler
from lsst.dax.apdb import Apdb, ApdbTables
from lsst.dax.apdb.cassandra import ApdbCassandra
from lsst.dax.apdb.cassandra.apdbCassandraSchema import ExtraTables
from lsst.dax.apdb.cassandra.cassandra_utils import select_concurrent
from lsst.dax.apdb.cassandra.queries import Column as C  # noqa: N817
from lsst.dax.apdb.cassandra.queries import Select
from lsst.sphgeom import Angle, UnitVector3d
from lsst.utils.iteration import chunk_iterable

_LOG = logging.getLogger(__name__)


class SourceRecord(NamedTuple):
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

    @property
    def cmp_val(self) -> tuple:
        return (
            self.apdb_part,
            self.diaSourceId,
            self.diaObjectId,
            self.ssObjectId,
            self.ra,
            self.dec,
            self.visit,
            self.detector,
            self.midpointMjdTai,
        )


class ReplicaSourceRecord(NamedTuple):
    diaSourceId: int
    diaObjectId: int | None
    ssObjectId: int | None
    apdb_replica_chunk: int
    apdb_replica_subchunk: int
    ra: float
    dec: float
    visit: int
    detector: int

    @property
    def cmp_val(self) -> tuple:
        return (
            self.diaSourceId,
            self.diaObjectId,
            self.ssObjectId,
            self.ra,
            self.dec,
            self.visit,
            self.detector,
        )

    @staticmethod
    def diffs(records: list[ReplicaSourceRecord]) -> dict[int, dict]:
        records = sorted(records, key=lambda r: r.apdb_replica_chunk)
        result: dict[int, dict] = defaultdict(dict)
        for attr in ("diaSourceId", "diaObjectId", "ssObjectId", "ra", "dec", "visit", "detector"):
            if len({getattr(record, attr) for record in records}) > 1:
                for record in records:
                    result[record.apdb_replica_chunk][attr] = getattr(record, attr)
        return result

    def diff2(self, records: list[SourceRecord]) -> dict[int, dict]:
        all_records = [self] + sorted(records, key=lambda r: r.midpointMjdTai)
        result: dict[int, dict] = defaultdict(dict)
        for attr in (
            "diaSourceId",
            "diaObjectId",
            "ssObjectId",
            "ra",
            "dec",
            "visit",
            "detector",
        ):
            if len({getattr(record, attr, None) for record in all_records}) > 1:
                for record in all_records:
                    result[getattr(record, "apdb_part", 0)][attr] = getattr(record, attr)
        return result


def find_duplicates(apdb_config: str) -> None:
    """Find duplicated DiaSources in replica table.

    Parameters
    ----------
    apdb_config : `str`
        APDB configuration location.
    """
    # No need to instantiate Apdb, we can look at config.
    apdb = Apdb.from_uri(apdb_config)
    assert isinstance(apdb, ApdbCassandra), "Expecting Cassandra APDB"

    # Get the list of chunks.
    chunks = apdb.get_replica().getReplicaChunks() or []
    _LOG.info("Found %d replica chunks", len(chunks))
    if not chunks:
        return

    context = apdb._context
    assert context.has_chunk_sub_partitions, "Must have subchunks"

    config = context.config

    # First run query that only finds duplicated diaSourceIds.
    table_name = context.schema.tableName(ExtraTables.replica_chunk_tables(True)[ApdbTables.DiaSource])

    query = Select(config.keyspace, table_name, ["diaSourceId"])
    query = query.where(C("apdb_replica_chunk") == 0)
    query = query.where(C("apdb_replica_subchunk") == 0)
    statement = context.stmt_factory(query, prepare=True)

    queries: list[tuple] = []
    for chunk in chunks:
        for subchunk in range(config.replica_sub_chunk_count):
            queries.append((statement, (chunk.id, subchunk)))

    records = cast(
        list[tuple[int]],
        select_concurrent(
            context.session,
            queries,
            "read_tuples",
            config.connection_config.read_concurrency,
        ),
    )
    counters = Counter(row[0] for row in records)
    duplicate_ids = {src_id for src_id, count in counters.items() if count > 1}
    _LOG.info("Found %d duplicate DiaSources", len(duplicate_ids))
    if not duplicate_ids:
        return

    # Now get more info about each duplicate
    columns = [
        "diaSourceId",
        "diaObjectId",
        "ssObjectId",
        "apdb_replica_chunk",
        "apdb_replica_subchunk",
        "ra",
        "dec",
        "visit",
        "detector",
    ]
    query = Select(config.keyspace, table_name, columns)
    query = query.where(C("apdb_replica_chunk") == 0)
    query = query.where(C("apdb_replica_subchunk") == 0)
    statement = context.stmt_factory(query, prepare=True)

    queries = []
    for chunk in chunks:
        for subchunk in range(config.replica_sub_chunk_count):
            queries.append((statement, (chunk.id, subchunk)))

    results = cassandra.concurrent.execute_concurrent(
        context.session,
        queries,
        results_generator=True,
        raise_on_first_error=False,
        concurrency=config.connection_config.read_concurrency,
        execution_profile="read_tuples",
    )
    rows: list[ReplicaSourceRecord] = []
    for success, result in results:
        if success:
            rows.extend(ReplicaSourceRecord(*row) for row in result if row[0] in duplicate_ids)
        else:
            _LOG.error("error returned by query: %s", result)
            raise result

    # Sort it all by diaSourceId and chunk ID.
    rows.sort(key=lambda r: (r.diaSourceId, r.apdb_replica_chunk))

    # Dump everything.
    writer = csv.writer(sys.stdout)
    writer.writerow(columns)
    writer.writerows(rows)


def find_matching_sources(csv_file: str, butler_config: str, apdb_config: str) -> None:
    """Find matching DiaSources in regular table.

    Parameters
    ----------
    csv_file : `str`
        Path to CSV file produced by `find`.
    butler_config : `str`
        Butler configuration location.
    apdb_config : `str`
        APDB configuration location.
    """
    source_ids: set[int] = set()
    visits: set[int] = set()
    ra_decs = set()
    for record in _read_replica_records(csv_file):
        source_ids.add(record.diaSourceId)
        ra_decs.add((record.ra, record.dec))
        visits.add(record.visit)
    _LOG.info("Found %d source IDs", len(source_ids))
    _LOG.info("Found %d visits", len(visits))

    butler = Butler.from_config(butler_config)
    instrument = "LSSTCam"
    visit_regions: list = []
    for visits_chunk in chunk_iterable(visits, 20):
        visit_records = butler.query_dimension_records(
            "visit",
            instrument=instrument,
            where="visit IN (:visits)",
            bind={"visits": visits_chunk},
        )
        assert len(visit_records) == len(visits_chunk)
        visit_regions.extend(vr.region for vr in visit_records)

    _LOG.info("Found %d regions", len(visit_regions))

    # No need to instantiate Apdb, we can look at config.
    apdb = Apdb.from_uri(apdb_config)
    assert isinstance(apdb, ApdbCassandra), "Expecting Cassandra APDB"

    # Find all spatial partitions.
    context = apdb._context
    partitioner = context.partitioner
    region_pixels = set()
    for region in visit_regions:
        region_pixels.update(partitioner.pixelization.pixels(region))
    _LOG.info("Found %d regions pixels", len(region_pixels))

    source_pixels = set()
    for ra, dec in ra_decs:
        direction = UnitVector3d(Angle.fromDegrees(ra), Angle.fromDegrees(dec))
        source_pixels.add(partitioner.pixelization.pixel(direction))
    _LOG.info("Found %d source pixels", len(source_pixels))

    time_part_start = partitioner.time_partition(Time("2026-02-18T00:00:00", format="isot"))
    time_part_end = partitioner.time_partition(Time("2026-02-28T00:00:00", format="isot"))
    time_partitions = list(range(time_part_start, time_part_end + 1))
    _LOG.info("Time partitions %s", time_partitions)

    columns = [
        "apdb_part",
        "diaSourceId",
        "diaObjectId",
        "ssObjectId",
        "ra",
        "dec",
        "visit",
        "detector",
        "midpointMjdTai",
    ]

    records: list[SourceRecord] = []
    for time_partition in time_partitions:
        table_name = context.schema.tableName(ApdbTables.DiaSource, time_partition)
        statement = context.stmt_factory(
            Select(context.config.keyspace, table_name, columns).where(C("apdb_part") == 0), prepare=True
        )
        queries = [(statement, (pixel,)) for pixel in source_pixels]

        results = cassandra.concurrent.execute_concurrent(
            context.session,
            queries,
            results_generator=True,
            raise_on_first_error=False,
            concurrency=context.config.connection_config.read_concurrency,
            execution_profile="read_tuples",
        )
        for success, result in results:
            if success:
                records.extend(SourceRecord(time_partition, *row) for row in result if row[1] in source_ids)
            else:
                _LOG.error("error returned by query: %s", result)
                raise result

    records = sorted(records, key=lambda r: (r.diaSourceId, r.midpointMjdTai))
    record_ids = {record.diaSourceId for record in records}
    _LOG.info("Found %d DiaSource records from %d unique sources", len(records), len(record_ids))

    # Dump everything.
    writer = csv.writer(sys.stdout)
    writer.writerow(["time_part"] + columns)
    writer.writerows(records)


def analyze_file(replica_file: str, source_file: str) -> None:
    """Analyze files produced by `find` and `find_matching_sources`.

    Parameters
    ----------
    replica_file : `str`
        Path to CSV file produced by `find`.
    source_file : `str`
        Path to CSV file produced by `find_matching_sources`.
    """
    replica_records: dict[int, list[ReplicaSourceRecord]] = defaultdict(list)
    source_records: dict[int, list[SourceRecord]] = defaultdict(list)
    for rrecord in _read_replica_records(replica_file):
        replica_records[rrecord.diaSourceId].append(rrecord)
    for record in _read_source_records(source_file):
        source_records[record.diaSourceId].append(record)

    for src_id, id_records in replica_records.items():
        id_records.sort(key=lambda r: r.apdb_replica_chunk)
        if len({record.cmp_val for record in id_records}) == 1:
            chunks = [str(record.apdb_replica_chunk) for record in id_records]
            print(f"{src_id}: {len(id_records)} identical records, chunks: {' '.join(chunks)}")
        else:
            print(f"{src_id}: {len(id_records)} different records")
            diffs = ReplicaSourceRecord.diffs(id_records)
            for chunk, diff in diffs.items():
                fdiff = " ".join(f"{attr}={val}" for attr, val in diff.items())
                print(f"    chunk={chunk} {fdiff}")

        first_rec = id_records[0]
        diffs = first_rec.diff2(source_records[src_id])
        for apdb_part, diff in diffs.items():
            fdiff = " ".join(f"{attr}={val}" for attr, val in diff.items())
            if apdb_part == 0:
                print(f"    replica record:    {fdiff}")
            else:
                print(f"    apdb_part={apdb_part} {fdiff}")


def _read_replica_records(path: str) -> Iterator[ReplicaSourceRecord]:
    with open(path, newline="") as csv_file:
        for row in csv.DictReader(csv_file):
            yield ReplicaSourceRecord(
                diaSourceId=int(row["diaSourceId"]),
                diaObjectId=int(row["diaObjectId"]) if row["diaObjectId"] else None,
                ssObjectId=int(row["ssObjectId"]) if row["ssObjectId"] else None,
                apdb_replica_chunk=int(row["apdb_replica_chunk"]),
                apdb_replica_subchunk=int(row["apdb_replica_subchunk"]),
                ra=float(row["ra"]),
                dec=float(row["dec"]),
                visit=int(row["visit"]),
                detector=int(row["detector"]),
            )


def _read_source_records(path: str) -> Iterator[SourceRecord]:
    with open(path, newline="") as csv_file:
        for row in csv.DictReader(csv_file):
            yield SourceRecord(
                time_part=int(row["time_part"]),
                apdb_part=int(row["apdb_part"]),
                diaSourceId=int(row["diaSourceId"]),
                diaObjectId=int(row["diaObjectId"]) if row["diaObjectId"] else None,
                ssObjectId=int(row["ssObjectId"]) if row["ssObjectId"] else None,
                ra=float(row["ra"]),
                dec=float(row["dec"]),
                visit=int(row["visit"]),
                detector=int(row["detector"]),
                midpointMjdTai=float(row["midpointMjdTai"]),
            )
