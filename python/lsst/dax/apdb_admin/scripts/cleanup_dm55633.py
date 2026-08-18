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

__all__ = []

import csv
import logging
import sys
from collections import Counter, defaultdict
from collections.abc import Generator, Iterable
from typing import Any, Literal, NamedTuple, Protocol, cast

import cassandra.concurrent
from astropy.time import Time

from lsst.dax.apdb import Apdb, ApdbTables
from lsst.dax.apdb.cassandra import ApdbCassandra
from lsst.dax.apdb.cassandra.apdbCassandraSchema import ExtraTables
from lsst.dax.apdb.cassandra.cassandra_utils import execute_concurrent, select_concurrent
from lsst.dax.apdb.cassandra.queries import Column as C  # noqa: N817
from lsst.dax.apdb.cassandra.queries import Delete, Insert, Select
from lsst.utils.iteration import chunk_iterable

_LOG = logging.getLogger(__name__)


class VisitDetector(NamedTuple):
    """Visit and detector tuple with a few helper methods."""

    visit: int
    detector: int

    @classmethod
    def from_row(cls, row: Any) -> VisitDetector:
        return cls(visit=row.visit, detector=row.detector)

    @classmethod
    def from_file(cls, path: str) -> Generator[tuple[VisitDetector, tuple[int, ...]]]:
        with open(path, newline="") as file:
            for line in file:
                words = line.strip().split()
                yield (VisitDetector(int(words[0]), int(words[1])), tuple(sorted(int(w) for w in words[2:])))

    @classmethod
    def dump(cls, vds: Iterable[tuple[VisitDetector, Iterable[int]]]) -> None:
        for vd, vd_chunks in sorted(vds):
            fmt_chunks = " ".join(str(chunk) for chunk in sorted(vd_chunks))
            print(f"{vd.visit} {vd.detector:3d} {fmt_chunks}")


class DiaSource(Protocol):
    """Protocol for records in regular DiaSource table."""

    apdb_part: int
    diaSourceId: int
    ra: float
    dec: float
    diaObjectId: int
    ssObjectId: int
    visit: int
    detector: int
    midpointMjdTai: float

    def _asdict(self) -> dict[str, Any]: ...

    @property
    def _fields(self) -> tuple[str, ...]: ...


class DiaSourceReplica(Protocol):
    """Protocol for records in DiaSourceChunks table."""

    apdb_replica_chunk: int
    apdb_replica_subchunk: int
    diaSourceId: int
    ra: float
    dec: float
    diaObjectId: int
    ssObjectId: int
    visit: int
    detector: int
    midpointMjdTai: float

    def _asdict(self) -> dict[str, Any]: ...

    @property
    def _fields(self) -> tuple[str, ...]: ...


class DiaObjectReplica(Protocol):
    """Protocol for records in DiaObjectChunks table."""

    apdb_replica_chunk: int
    apdb_replica_subchunk: int
    diaObjectId: int
    validityStartMjdTai: float
    ra: float
    dec: float
    nDiaSources: int

    def _asdict(self) -> dict[str, Any]: ...

    @property
    def _fields(self) -> tuple[str, ...]: ...


class SourceRecord(NamedTuple):
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
    def from_row(cls, time_part: int, row: DiaSource) -> SourceRecord:
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

    @classmethod
    def from_csv(cls, path: str) -> Generator[SourceRecord]:
        with open(path, newline="") as csv_file:
            for row in csv.DictReader(csv_file):
                yield cls.from_csv_dict(row)

    @classmethod
    def from_csv_dict(cls, csv_dict: dict[str, str]) -> SourceRecord:
        return SourceRecord(
            time_part=int(csv_dict["time_part"]),
            apdb_part=int(csv_dict["apdb_part"]),
            diaSourceId=int(csv_dict["diaSourceId"]),
            diaObjectId=int(csv_dict["diaObjectId"]) if csv_dict["diaObjectId"] else None,
            ssObjectId=int(csv_dict["ssObjectId"]) if csv_dict["ssObjectId"] else None,
            ra=float(csv_dict["ra"]),
            dec=float(csv_dict["dec"]),
            visit=int(csv_dict["visit"]),
            detector=int(csv_dict["detector"]),
            midpointMjdTai=float(csv_dict["midpointMjdTai"]),
        )

    @classmethod
    def to_csv(cls, records: Iterable[SourceRecord]) -> None:
        writer = csv.writer(sys.stdout)
        writer.writerow(cls._fields)
        writer.writerows(records)


class ReplicaSourceRecord(NamedTuple):
    """Subset of attributes of replica DiaSource records."""

    diaSourceId: int
    diaObjectId: int | None
    ssObjectId: int | None
    apdb_replica_chunk: int
    apdb_replica_subchunk: int
    ra: float
    dec: float
    visit: int
    detector: int

    @classmethod
    def from_row(cls, row: DiaSourceReplica) -> ReplicaSourceRecord:
        return cls(
            diaSourceId=row.diaSourceId,
            diaObjectId=row.diaObjectId,
            ssObjectId=row.ssObjectId,
            apdb_replica_chunk=row.apdb_replica_chunk,
            apdb_replica_subchunk=row.apdb_replica_subchunk,
            ra=row.ra,
            dec=row.dec,
            visit=row.visit,
            detector=row.detector,
        )

    @classmethod
    def from_csv(cls, path: str) -> Generator[ReplicaSourceRecord]:
        with open(path, newline="") as csv_file:
            for row in csv.DictReader(csv_file):
                yield cls.from_csv_dict(row)

    @classmethod
    def from_csv_dict(cls, csv_dict: dict[str, str]) -> ReplicaSourceRecord:
        return cls(
            diaSourceId=int(csv_dict["diaSourceId"]),
            diaObjectId=int(csv_dict["diaObjectId"]) if csv_dict["diaObjectId"] else None,
            ssObjectId=int(csv_dict["ssObjectId"]) if csv_dict["ssObjectId"] else None,
            apdb_replica_chunk=int(csv_dict["apdb_replica_chunk"]),
            apdb_replica_subchunk=int(csv_dict["apdb_replica_subchunk"]),
            ra=float(csv_dict["ra"]),
            dec=float(csv_dict["dec"]),
            visit=int(csv_dict["visit"]),
            detector=int(csv_dict["detector"]),
        )

    @classmethod
    def to_csv(cls, records: Iterable[ReplicaSourceRecord]) -> None:
        writer = csv.writer(sys.stdout)
        writer.writerow(cls._fields)
        writer.writerows(records)


class ReplicaObjectRecord(NamedTuple):
    """Subset of attributes of replica DiaObject records."""

    diaObjectId: int
    validityStartMjdTai: float
    apdb_replica_chunk: int
    apdb_replica_subchunk: int
    ra: float
    dec: float
    nDiaSources: int

    @classmethod
    def from_row(cls, row: DiaObjectReplica) -> ReplicaObjectRecord:
        return cls(
            diaObjectId=row.diaObjectId,
            validityStartMjdTai=row.validityStartMjdTai,
            apdb_replica_chunk=row.apdb_replica_chunk,
            apdb_replica_subchunk=row.apdb_replica_subchunk,
            ra=row.ra,
            dec=row.dec,
            nDiaSources=row.nDiaSources,
        )

    @classmethod
    def from_csv(cls, path: str) -> Generator[ReplicaObjectRecord]:
        with open(path, newline="") as csv_file:
            for row in csv.DictReader(csv_file):
                yield cls.from_csv_dict(row)

    @classmethod
    def from_csv_dict(cls, csv_dict: dict[str, str]) -> ReplicaObjectRecord:
        return cls(
            diaObjectId=int(csv_dict["diaObjectId"]),
            validityStartMjdTai=float(csv_dict["validityStartMjdTai"]),
            apdb_replica_chunk=int(csv_dict["apdb_replica_chunk"]),
            apdb_replica_subchunk=int(csv_dict["apdb_replica_subchunk"]),
            ra=float(csv_dict["ra"]),
            dec=float(csv_dict["dec"]),
            nDiaSources=int(csv_dict["nDiaSources"]),
        )


def find_visit_detector(apdb_config: str) -> None:
    """Find visit-detector combinations that were processed more than once.

    Parameters
    ----------
    apdb_config : `str`
        APDB configuration location.
    """
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

    table_name = context.schema.tableName(ExtraTables.replica_chunk_tables(True)[ApdbTables.DiaSource])

    query = Select(config.keyspace, table_name, ["visit", "detector", "apdb_replica_chunk"])
    query = query.where(C("apdb_replica_chunk") == 0)
    query = query.where(C("apdb_replica_subchunk") == 0)
    statement = context.stmt_factory(query, prepare=True)

    queries: list[tuple] = []
    for chunk in chunks:
        for subchunk in range(config.replica_sub_chunk_count):
            queries.append((statement, (chunk.id, subchunk)))

    records = cast(
        list,
        select_concurrent(
            context.session,
            queries,
            "read_named_tuples",
            config.connection_config.read_concurrency,
        ),
    )
    vd_map: dict[VisitDetector, set[int]] = defaultdict(set)
    for row in records:
        vd_map[VisitDetector.from_row(row)].add(row.apdb_replica_chunk)

    # Dump entries with more than one processing.
    vds = [(vd, chunks) for vd, chunks in vd_map.items() if len(chunks) > 1]
    visits = {vd.visit for vd, _ in vds}
    _LOG.info("Found %d visit-detectors from %d visits", len(vds), len(visits))

    VisitDetector.dump(vds)


def sources_to_delete(apdb_config: str, visit_detector: str) -> None:
    """Find DiaSources to be deleted.

    Parameters
    ----------
    apdb_config : `str`
        APDB configuration location.
    visit_detector : `str`
        Path to visit-detector file produced by `find_visit_detector`.
    """
    rows = _sources_to_keep_or_drop(apdb_config, visit_detector, "drop")

    # Dump everything.
    ReplicaSourceRecord.to_csv(rows)


def sources_to_keep(apdb_config: str, visit_detector: str) -> None:
    """Find DiaSources to keep.

    Parameters
    ----------
    apdb_config : `str`
        APDB configuration location.
    visit_detector : `str`
        Path to visit-detector file produced by `find_visit_detector`.
    """
    rows = _sources_to_keep_or_drop(apdb_config, visit_detector, "keep")

    # Dump everything.
    ReplicaSourceRecord.to_csv(rows)


def _sources_to_keep_or_drop(
    apdb_config: str, visit_detector: str, action: Literal["keep", "drop"]
) -> list[ReplicaSourceRecord]:
    apdb = Apdb.from_uri(apdb_config)
    assert isinstance(apdb, ApdbCassandra), "Expecting Cassandra APDB"

    context = apdb._context
    assert context.has_chunk_sub_partitions, "Must have subchunks"

    config = context.config

    vd_by_chunk: dict[int, set[VisitDetector]] = defaultdict(set)
    count = 0
    for vd, chunks in VisitDetector.from_file(visit_detector):
        # Earliest chunk is the one to keep.
        if action == "keep":
            vd_by_chunk[sorted(chunks)[0]].add(vd)
        else:
            for chunk in sorted(chunks)[1:]:
                vd_by_chunk[chunk].add(vd)
        count += 1

    _LOG.info("Loaded %d visit-detectors", count)

    # First run query that only finds duplicated diaSourceIds.
    table_name = context.schema.tableName(ExtraTables.replica_chunk_tables(True)[ApdbTables.DiaSource])

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

    queries: list[tuple] = []
    for chunk in vd_by_chunk:
        for subchunk in range(config.replica_sub_chunk_count):
            queries.append((statement, (chunk, subchunk)))

    results = cassandra.concurrent.execute_concurrent(
        context.session,
        queries,
        results_generator=True,
        raise_on_first_error=False,
        concurrency=config.connection_config.read_concurrency,
        execution_profile="read_named_tuples",
    )
    rows: list[ReplicaSourceRecord] = []
    for success, result in results:
        if success:
            for row in result:
                vd = VisitDetector.from_row(row)
                chunk = cast(int, row.apdb_replica_chunk)
                if vd in vd_by_chunk[chunk]:
                    rows.append(ReplicaSourceRecord.from_row(row))
        else:
            _LOG.error("error returned by query: %s", result)
            raise result

    _LOG.info("Found %d DiaSources", len(rows))

    # Sort it all by diaSourceId and chunk ID.
    rows.sort(key=lambda r: (r.diaSourceId, r.apdb_replica_chunk))
    return rows


def find_regular_sources(apdb_config: str, csv_file: str) -> None:
    """Find matching DiaSources in regular table.

    Parameters
    ----------
    apdb_config : `str`
        APDB configuration location.
    csv_file : `str`
        Path to CSV file produced by ``sources_to_keep/delete``.
    """
    source_ids: set[int] = set()
    visits: set[int] = set()
    ra_decs = set()
    for record in ReplicaSourceRecord.from_csv(csv_file):
        source_ids.add(record.diaSourceId)
        ra_decs.add((record.ra, record.dec))
        visits.add(record.visit)
    _LOG.info("Found %d source IDs", len(source_ids))
    _LOG.info("Found %d visits", len(visits))

    # No need to instantiate Apdb, we can look at config.
    apdb = Apdb.from_uri(apdb_config)
    assert isinstance(apdb, ApdbCassandra), "Expecting Cassandra APDB"
    context = apdb._context
    partitioner = context.partitioner

    # Find all spatial partitions.
    pixels = {partitioner.pixel(ra, dec) for ra, dec in ra_decs}
    _LOG.info("Found %d source pixels", len(pixels))

    time_part_start = partitioner.time_partition(Time("2026-02-18T00:00:00", format="isot"))
    time_part_end = partitioner.time_partition(Time("2026-03-10T00:00:00", format="isot"))
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
        queries = [(statement, (pixel,)) for pixel in pixels]

        results = cassandra.concurrent.execute_concurrent(
            context.session,
            queries,
            results_generator=True,
            raise_on_first_error=False,
            concurrency=context.config.connection_config.read_concurrency,
            execution_profile="read_named_tuples",
        )
        for success, result in results:
            if success:
                records.extend(
                    SourceRecord.from_row(time_partition, row)
                    for row in result
                    if row.diaSourceId in source_ids
                )
            else:
                _LOG.error("error returned by query: %s", result)
                raise result

    records = sorted(records, key=lambda r: (r.diaSourceId, r.midpointMjdTai, r.apdb_part))
    record_ids = {record.diaSourceId for record in records}
    _LOG.info("Found %d DiaSource records from %d unique sources", len(records), len(record_ids))

    # Dump everything.
    SourceRecord.to_csv(records)


def find_replica_objects(apdb_config: str, csv_file: str) -> None:
    """Find matching DiaObjects in replica table.

    Parameters
    ----------
    apdb_config : `str`
        APDB configuration location.
    csv_file : `str`
        Path to CSV file produced by `sources_to_delete/keep`.
    """
    objects_by_chunk: dict[int, set[int]] = defaultdict(set)
    sources_by_object: dict[tuple[int, int], list[int]] = defaultdict(list)
    source_ids = set()
    object_ids = set()
    object_chunk_ids = set()
    sources = list(ReplicaSourceRecord.from_csv(csv_file))
    for source in sources:
        if source.diaObjectId is not None:
            objects_by_chunk[source.apdb_replica_chunk].add(source.diaObjectId)
            object_ids.add(source.diaObjectId)
            object_chunk_ids.add((source.diaObjectId, source.apdb_replica_chunk))
            sources_by_object[(source.diaObjectId, source.apdb_replica_chunk)].append(source.diaSourceId)
        source_ids.add(source.diaSourceId)

    _LOG.info(
        "Loaded %d sources with %d unique IDs, %d DiaObject IDs and %d object/chunk IDs",
        len(sources),
        len(source_ids),
        len(object_ids),
        len(object_chunk_ids),
    )

    # No need to instantiate Apdb, we can look at config.
    apdb = Apdb.from_uri(apdb_config)
    assert isinstance(apdb, ApdbCassandra), "Expecting Cassandra APDB"

    context = apdb._context
    assert context.has_chunk_sub_partitions, "Must have subchunks"

    config = context.config

    # First run query that only finds duplicated diaSourceIds.
    table_name = context.schema.tableName(ExtraTables.replica_chunk_tables(True)[ApdbTables.DiaObject])

    columns = [
        "diaObjectId",
        "validityStartMjdTai",
        "apdb_replica_chunk",
        "apdb_replica_subchunk",
        "ra",
        "dec",
        "nDiaSources",
    ]

    query = (
        Select(config.keyspace, table_name, columns)
        .where(C("apdb_replica_chunk") == 0)
        .where(C("apdb_replica_subchunk") == 0)
    )
    statement = context.stmt_factory(query, prepare=True)

    queries: list[tuple] = []
    for chunk in objects_by_chunk:
        for subchunk in range(config.replica_sub_chunk_count):
            queries.append((statement, (chunk, subchunk)))

    _LOG.info("Generated %d queries", len(queries))

    results = cassandra.concurrent.execute_concurrent(
        context.session,
        queries,
        results_generator=True,
        raise_on_first_error=False,
        concurrency=config.connection_config.read_concurrency,
        execution_profile="read_named_tuples",
    )
    rows: list[ReplicaObjectRecord] = []
    for success, result in results:
        if success:
            for row in result:
                rec = ReplicaObjectRecord.from_row(row)
                if rec.diaObjectId in objects_by_chunk[rec.apdb_replica_chunk]:
                    rows.append(rec)
        else:
            _LOG.error("error returned by query: %s", result)
            raise result

    _LOG.info("Found %d DiaObjects", len(rows))

    # Sort it all by diaObjectId and validityStartMjdTai.
    rows.sort(key=lambda r: (r.diaObjectId, r.validityStartMjdTai))

    # Compare number of object versions and number of sources in each chunk.
    counts_by_chunk = Counter((r.diaObjectId, r.apdb_replica_chunk) for r in rows)
    objects_ambiguous = set()
    for key, object_count in counts_by_chunk.items():
        source_count = len(sources_by_object[key])
        if object_count != source_count:
            objects_ambiguous.add(key)
            _LOG.error(
                "object and source counts are different for diaObjectId=%s and chunk=%s: %d vs %d",
                key[0],
                key[1],
                object_count,
                source_count,
            )

    # Dump everything.
    writer = csv.writer(sys.stdout)
    writer.writerow(columns + ["flag"])
    for rec in rows:
        flag = "AMB" if (rec.diaObjectId, rec.apdb_replica_chunk) in objects_ambiguous else "-"
        writer.writerow(list(rec) + [flag])


def cleanup_sources(apdb_config: str, visit_detector: str, update: bool) -> None:
    """Do cleanup of DiaSources in both regular and replica tables.

    Parameters
    ----------
    apdb_config : `str`
        APDB configuration location.
    visit_detector : `str`
        File produced by `find_visit_detector`.
    update : `bool`
        When True do actual updates, by default only print actions.

    Notes
    -----
    This method uses the list of visits/detectors as an input. The sequence
    of actions:

        - Find DiaSources to keep or drop in replica table.
        - Find matching DiaSources in regular table.
        - Drop and re-create DiaSources in regular table.
        - Drop DiaSources in replica table.

    After this only the DiaSources from the first processing of a particular
    visit/detector will remain in the database.
    """
    apdb = Apdb.from_uri(apdb_config)
    assert isinstance(apdb, ApdbCassandra), "Expecting Cassandra APDB"

    # Get DiaSources to keep and drop from replica table.
    to_keep, to_drop = _find_replica_sources(apdb, visit_detector)

    # Drop/re-create DiaSources in regular table.
    _recreate_regular_sources(apdb, to_keep, to_drop, update)

    # Drop duplicated processing from replica table.
    _drop_replica_sources(apdb, to_drop, update)


def _find_replica_sources(
    apdb: ApdbCassandra, visit_detector: str
) -> tuple[list[DiaSourceReplica], list[DiaSourceReplica]]:
    """Find full DiaSource records from replica table to keep and drop.

    Parameters
    ----------
    apdb : `ApdbCassandra`
        APDB instance.
    visit_detector : `str`
        Loaction of the file name with visit/detector info.
    """
    all_chunks: set[int] = set()
    chunks_to_keep: dict[VisitDetector, int] = {}
    chunks_to_drop: dict[VisitDetector, list[int]] = {}
    count = 0
    for vd, chunks in VisitDetector.from_file(visit_detector):
        # Keep DiaSources from earliest chunk, sort chunks for easy check.
        all_chunks.update(chunks)
        sorted_chunks = sorted(chunks)
        chunks_to_keep[vd] = sorted_chunks[0]
        chunks_to_drop[vd] = sorted_chunks[1:]
        count += 1
    _LOG.info("Loaded %d visit-detectors", count)

    context = apdb._context
    assert context.has_chunk_sub_partitions, "Must have subchunks"

    config = context.config

    # First run query that only finds duplicated diaSourceIds.
    table_name = context.schema.tableName(ExtraTables.replica_chunk_tables(True)[ApdbTables.DiaSource])

    query = Select(config.keyspace, table_name, ["*"])
    query = query.where(C("apdb_replica_chunk") == 0)
    query = query.where(C("apdb_replica_subchunk") == 0)
    statement = context.stmt_factory(query, prepare=True)

    queries: list[tuple] = []
    for chunk in all_chunks:
        for subchunk in range(config.replica_sub_chunk_count):
            queries.append((statement, (chunk, subchunk)))

    results = cassandra.concurrent.execute_concurrent(
        context.session,
        queries,
        results_generator=True,
        raise_on_first_error=False,
        concurrency=config.connection_config.read_concurrency,
        execution_profile="read_named_tuples",
    )
    to_keep: list[DiaSourceReplica] = []
    to_drop: list[DiaSourceReplica] = []
    counts_to_keep: Counter = Counter()
    counts_to_drop: Counter = Counter()
    for success, result in results:
        if success:
            for row in result:
                vd = VisitDetector.from_row(row)
                if vd in chunks_to_keep:
                    chunk = cast(int, row.apdb_replica_chunk)
                    if chunk == chunks_to_keep[vd]:
                        to_keep.append(row)
                        counts_to_keep[vd] += 1
                    elif chunk in chunks_to_drop[vd]:
                        to_drop.append(row)
                        counts_to_drop[vd] += 1
        else:
            _LOG.error("error returned by query: %s", result)
            raise result

    _LOG.info(
        "Found %d DiaSources to keep (%d unique IDs) and %d to drop (%d unique IDs)",
        len(to_keep),
        len({record.diaSourceId for record in to_keep}),
        len(to_drop),
        len({record.diaSourceId for record in to_drop}),
    )

    def _chunk_to_time(chunk: int) -> str:
        t = Time(chunk, format="unix_tai")
        return str(t.isot)

    if _LOG.isEnabledFor(logging.DEBUG):
        for vd in sorted(counts_to_keep):
            n_to_keep = counts_to_keep[vd]
            n_to_drop = counts_to_drop[vd]
            ch_to_drop = [_chunk_to_time(ch) for ch in chunks_to_drop[vd]]
            flag = ""
            if n_to_keep < n_to_drop:
                flag = " <"
            elif n_to_keep > n_to_drop:
                flag = " >"
            _LOG.debug(f"{vd[0]} {vd[1]:3d} {ch_to_drop} {n_to_keep:4d} {n_to_drop:4d}{flag}")

    overlap = set(to_keep) & set(to_drop)
    _LOG.info("Number of overlapping records: %d", len(overlap))

    return to_keep, to_drop


def _recreate_regular_sources(
    apdb: ApdbCassandra, to_keep: list[DiaSourceReplica], to_drop: list[DiaSourceReplica], update: bool
) -> None:
    """Recreate records in the regular DiaSource tables.

    Parameters
    ----------
    apdb
        Cassandra APDB instance.
    to_keep
        List of records from replica tables that we have to keep - these
        records were created on the initial processing (in PP).
    to_drop
        List of records from replica tables that we have to drop - these
        records were created by daytime re-processing.
    update
        If `False` then skip actual updates.
    """
    # After the call to this method:
    #  - all records matching `to_drop` records should be dropped,
    #  - deleted or changed records in `to_keep` must be recreated.
    #
    # "Matching" in this case means matching ``diaObjectId`` because it is the
    # unique identifier in the logical model. Actual schema cannot guarantee
    # its uniqueness because we partition DiaSources both temporally and
    # spatially. We need to look for matching diaSourceIds in all partitions.
    #
    # Most straightforward way to do this is to drop all records in all
    # partitions that match ``diaObjectId`` in any of the two lists and then
    # re-create all records in ``to_keep`` list. But it could create too many
    # tombstones which is not ideal. Instead we use a different approach:
    #  - find all records in all partitions that match ``diaSourceId`` in
    #    any of the two lists.
    #  - for each ``daSourceId``:
    #    - drop records that do not match a record in ``to_keep``
    #    - if there are no records that match a record in ``to_keep`` then
    #      recreate thar record from ``to_keep``

    _LOG.info("Searching for matching sources to keep")
    matches_to_keep = _find_regular_sources(apdb, to_keep)
    _LOG.info("Searching for matching sources to drop")
    matches_to_drop = _find_regular_sources(apdb, to_drop)

    _LOG.info("Number of overlapping records: %d", len(set(matches_to_keep) & set(matches_to_drop)))

    ids_to_keep = {record.diaSourceId: record for record in to_keep}
    assert len(ids_to_keep) == len(to_keep), "All to_keep IDs must be unique"

    # ids_to_drop: dict[int, list[DiaSourceReplica]] = defaultdict(list)
    # for record in to_drop:
    #     ids_to_drop[record.diaSourceId].append(record)

    all_matches = set(matches_to_keep) | set(matches_to_drop)
    will_keep = []
    will_drop = []
    for record in all_matches:
        if (match := ids_to_keep.get(record.diaSourceId)) and _same_record(record, match):
            # We are going to keep this one, but remove it from ids_to_keep
            # so that we know which records we have to recreate.
            will_keep.append(record)
            del ids_to_keep[record.diaSourceId]
        else:
            will_drop.append(record)

    _LOG.info(
        "Will keep %d records, drop %d records, and re-create %d records",
        len(will_keep),
        len(will_drop),
        len(ids_to_keep),
    )

    # Records in ``will_drop`` have to be deleted.
    _drop_regular_records(apdb, will_drop, update)

    # Whatever is left in ids_to_keep will need to be re-created.
    _insert_regular_records(apdb, ids_to_keep.values(), update)


def _same_record(regular_record: DiaSource, replica_record: DiaSourceReplica) -> bool:
    # Compare two records ignoring difference in partitioning columns.

    regular_dict = regular_record._asdict()
    del regular_dict["apdb_part"]
    replica_dict = replica_record._asdict()
    del replica_dict["apdb_replica_chunk"]
    del replica_dict["apdb_replica_subchunk"]
    return regular_dict == replica_dict


def _drop_replica_sources(apdb: ApdbCassandra, to_drop: list[DiaSourceReplica], update: bool) -> None:
    # Delete records from DiaSource replica table.

    _LOG.info("Will drop %d sources from replica table", len(to_drop))

    sources_by_chunk: dict[tuple[int, int], list[int]] = defaultdict(list)
    for source in to_drop:
        sources_by_chunk[(source.apdb_replica_chunk, source.apdb_replica_subchunk)].append(source.diaSourceId)

    context = apdb._context
    config = context.config

    table_name = context.schema.tableName(ExtraTables.replica_chunk_tables(True)[ApdbTables.DiaSource])

    count_query = Select(
        config.keyspace, table_name, ["apdb_replica_chunk", "apdb_replica_subchunk", "count(*)"]
    )
    count_query = count_query.where(C("apdb_replica_chunk") == 0)
    count_query = count_query.where(C("apdb_replica_subchunk") == 0)
    count_stmt = context.stmt_factory(count_query, prepare=True)

    # Wind total number of records in each partition.
    stmts = []
    for chunk, subchunk in sources_by_chunk:
        stmts.append((count_stmt, (chunk, subchunk)))

    results = cassandra.concurrent.execute_concurrent(
        context.session,
        stmts,
        results_generator=True,
        raise_on_first_error=False,
        concurrency=config.connection_config.read_concurrency,
        execution_profile="read_tuples",
    )

    db_counts = {}
    for success, result in results:
        if success:
            for chunk, subchunk, count in result:
                db_counts[(chunk, subchunk)] = count
        else:
            _LOG.error("error returned by query: %s", result)
            raise result

    query = (
        Delete(config.keyspace, table_name)
        .where(C("apdb_replica_chunk") == 0)
        .where(C("apdb_replica_subchunk") == 0)
    )
    drop_subchunk = context.stmt_factory(query, prepare=True)
    query = query.where(C("diaSourceId") == 0)
    drop_record = context.stmt_factory(query, prepare=True)

    queries: list[tuple[Delete, tuple]] = []
    for (chunk, subchunk), sources in sources_by_chunk.items():
        if len(sources) == db_counts[(chunk, subchunk)]:
            _LOG.debug("Will drop whole chunk/subchunk %d/%d", chunk, subchunk)
            queries.append((drop_subchunk, (chunk, subchunk)))
        else:
            _LOG.debug("Will drop %d sources for chunk/subchunk %d/%d", len(sources), chunk, subchunk)
            queries.extend((drop_record, (chunk, subchunk, diaSourceId)) for diaSourceId in sources)

    if update:
        for query_chunk in chunk_iterable(queries, 1000):
            execute_concurrent(context.session, list(query_chunk), execution_profile="write")
        _LOG.info("Executed %d DELETE queries", len(queries))
    else:
        _LOG.info("Would have executed %d DELETE queries", len(queries))


def _insert_regular_records(apdb: ApdbCassandra, records: Iterable[DiaSourceReplica], update: bool) -> None:
    # Use replica DiaSources to recreate regular DiaSources.

    context = apdb._context
    partitioner = context.partitioner

    # We need to iterate more than once.
    record_list = list(records)
    if not record_list:
        return

    # Get the list of regular columns in the DiaSource.
    columns = list(record_list[0]._fields)
    columns.remove("apdb_replica_chunk")
    columns.remove("apdb_replica_subchunk")

    time_partitions = {
        partitioner.time_partition(Time(record.midpointMjdTai, format="mjd", scale="tai"))
        for record in records
    }

    statements = {}
    for time_partition in time_partitions:
        table_name = context.schema.tableName(ApdbTables.DiaSource, time_partition)
        query = Insert(context.config.keyspace, table_name, ["apdb_part"] + columns)
        statements[time_partition] = context.stmt_factory(query, prepare=True)

    queries = []
    for record in records:
        time_partition = partitioner.time_partition(Time(record.midpointMjdTai, format="mjd", scale="tai"))
        apdb_part = partitioner.pixel(record.ra, record.dec)
        values = [apdb_part] + [getattr(record, column) for column in columns]
        queries.append((statements[time_partition], values))

    if update:
        for query_chunk in chunk_iterable(queries, 1000):
            execute_concurrent(context.session, list(query_chunk), execution_profile="write")
        _LOG.info("Inserted %d records", len(queries))
    else:
        _LOG.info("Would have inserted %d records", len(queries))


def _drop_regular_records(apdb: ApdbCassandra, records: list[DiaSource], update: bool) -> None:
    context = apdb._context
    partitioner = context.partitioner

    # All affected tables
    time_partitions = {
        partitioner.time_partition(Time(record.midpointMjdTai, format="mjd", scale="tai"))
        for record in records
    }
    _LOG.info("Will delete records in time partitions %s", time_partitions)

    statements = {}
    for time_partition in time_partitions:
        table_name = context.schema.tableName(ApdbTables.DiaSource, time_partition)
        query = (
            Delete(context.config.keyspace, table_name)
            .where(C("apdb_part") == 0)
            .where(C("diaSourceId") == 0)
        )
        statements[time_partition] = context.stmt_factory(query, prepare=True)

    queries = []
    for record in records:
        time_partition = partitioner.time_partition(Time(record.midpointMjdTai, format="mjd", scale="tai"))
        queries.append((statements[time_partition], (record.apdb_part, record.diaSourceId)))

    if update:
        for query_chunk in chunk_iterable(queries, 1000):
            execute_concurrent(context.session, list(query_chunk), execution_profile="write")
        _LOG.info("Dropped %d records", len(queries))
    else:
        _LOG.info("Would have dropped %d records", len(queries))


def _find_regular_sources(apdb: ApdbCassandra, sources: list[DiaSourceReplica]) -> list[DiaSource]:
    # Find matching DiaSources in regular table.
    source_ids: set[int] = set()
    visits: set[int] = set()
    ra_decs = set()
    midpoint_min = 100_000.0
    midpoint_max = 0.0
    for record in sources:
        source_ids.add(record.diaSourceId)
        ra_decs.add((record.ra, record.dec))
        visits.add(record.visit)
        if record.midpointMjdTai < midpoint_min:
            midpoint_min = record.midpointMjdTai
        if record.midpointMjdTai > midpoint_max:
            midpoint_max = record.midpointMjdTai
    _LOG.info("Found %d source IDs", len(source_ids))
    _LOG.info("Found %d visits", len(visits))

    context = apdb._context
    partitioner = context.partitioner

    # Find all spatial partitions.
    pixels = {partitioner.pixel(ra, dec) for ra, dec in ra_decs}
    _LOG.info("Found %d source pixels", len(pixels))

    time_part_start = partitioner.time_partition(Time(midpoint_min, format="mjd", scale="tai"))
    time_part_end = partitioner.time_partition(Time(midpoint_max, format="mjd", scale="tai"))
    time_partitions = list(range(time_part_start, time_part_end + 1))
    _LOG.info("Time partitions %s", time_partitions)

    records: list[DiaSource] = []
    for time_partition in time_partitions:
        table_name = context.schema.tableName(ApdbTables.DiaSource, time_partition)
        statement = context.stmt_factory(
            Select(context.config.keyspace, table_name, ["*"]).where(C("apdb_part") == 0), prepare=True
        )
        queries = [(statement, (pixel,)) for pixel in pixels]

        results = cassandra.concurrent.execute_concurrent(
            context.session,
            queries,
            results_generator=True,
            raise_on_first_error=False,
            concurrency=context.config.connection_config.read_concurrency,
            execution_profile="read_named_tuples",
        )
        for success, result in results:
            if success:
                records.extend(row for row in result if row.diaSourceId in source_ids)
            else:
                _LOG.error("error returned by query: %s", result)
                raise result

    records = sorted(records, key=lambda r: (r.diaSourceId, r.midpointMjdTai, r.apdb_part))
    record_ids = {record.diaSourceId for record in records}
    _LOG.info("Found %d DiaSource records from %d unique sources", len(records), len(record_ids))

    return records
