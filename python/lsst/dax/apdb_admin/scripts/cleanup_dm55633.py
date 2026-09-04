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
import io
import itertools
import json
import logging
import sys
from collections import Counter, defaultdict
from collections.abc import Generator, Iterable
from operator import attrgetter
from typing import Any, Literal, NamedTuple, Protocol, cast
from zipfile import ZIP_DEFLATED, ZipFile

import cassandra.concurrent
from astropy.time import Time

from lsst.dax.apdb import Apdb, ApdbTables
from lsst.dax.apdb.cassandra import ApdbCassandra
from lsst.dax.apdb.cassandra.apdbCassandraSchema import ExtraTables
from lsst.dax.apdb.cassandra.cassandra_utils import execute_concurrent, select_concurrent
from lsst.dax.apdb.cassandra.partitioner import Partitioner
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
    diaObjectId: int | None
    ssObjectId: int | None
    visit: int
    detector: int
    midpointMjdTai: float

    def _asdict(self) -> dict[str, Any]: ...

    @property
    def _fields(self) -> tuple[str, ...]: ...

    def _replace(self, **kwargs: Any) -> DiaSource: ...


def _fmt_src(rec: DiaSource) -> str:
    return (
        f"id={rec.diaSourceId} ra={rec.ra} dec={rec.dec} "
        f"part={rec.apdb_part} obj_id={rec.diaObjectId} ss_id={rec.ssObjectId}"
    )


class DiaSourceReplica(Protocol):
    """Protocol for records in DiaSourceChunks table."""

    apdb_replica_chunk: int
    apdb_replica_subchunk: int
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

    def _replace(self, **kwargs: Any) -> DiaSourceReplica: ...


def _fmt_src_rep(rec: DiaSourceReplica, partitioner: Any) -> str:
    part = partitioner.pixel(rec.ra, rec.dec)
    return (
        f"id={rec.diaSourceId} ra={rec.ra} dec={rec.dec} "
        f"part={part} obj_id={rec.diaObjectId} ss_id={rec.ssObjectId}"
    )


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


class SourceReassignRecord(NamedTuple):
    """Update payload for DisSource reassignment to DiaObject."""

    update_time_ns: int
    update_order: int
    apdb_replica_chunk: int
    apdb_replica_subchunk: int
    diaSourceId: int
    ra: float
    dec: float
    midpointMjdTai: float
    diaObjectId: int

    @classmethod
    def from_row(cls, row: Any) -> SourceReassignRecord | None:
        update_payload = json.loads(cast(str, row.update_payload))
        if update_payload["update_type"] != "reassign_diasource_to_diaobject":
            return None
        return cls(
            update_time_ns=row.update_time_ns,
            update_order=row.update_order,
            apdb_replica_chunk=row.apdb_replica_chunk,
            apdb_replica_subchunk=row.apdb_replica_subchunk,
            diaSourceId=update_payload["diaSourceId"],
            ra=update_payload["ra"],
            dec=update_payload["dec"],
            midpointMjdTai=update_payload["midpointMjdTai"],
            diaObjectId=update_payload["diaObjectId"],
        )

    def as_str(self, partitioner: Partitioner) -> str:
        part = partitioner.pixel(self.ra, self.dec)
        return f"id={self.diaSourceId} ra={self.ra} dec={self.dec} part={part} obj_id={self.diaObjectId}"


class CloseValidityRecord(NamedTuple):
    """Update payload for closing DiaObject validity."""

    update_time_ns: int
    update_order: int
    apdb_replica_chunk: int
    apdb_replica_subchunk: int
    diaObjectId: int
    ra: float
    dec: float
    validityEndMjdTai: float
    nDiaSources: int | None

    @classmethod
    def from_row(cls, row: Any) -> CloseValidityRecord | None:
        update_payload = json.loads(cast(str, row.update_payload))
        if update_payload["update_type"] != "close_diaobject_validity":
            return None
        return cls(
            update_time_ns=row.update_time_ns,
            update_order=row.update_order,
            apdb_replica_chunk=row.apdb_replica_chunk,
            apdb_replica_subchunk=row.apdb_replica_subchunk,
            diaObjectId=update_payload["diaObjectId"],
            ra=update_payload["ra"],
            dec=update_payload["dec"],
            validityEndMjdTai=update_payload["validityEndMjdTai"],
            nDiaSources=update_payload["nDiaSources"],
        )


class RecordUpdates(NamedTuple):
    keep_initial_records: list[DiaSourceReplica] = []
    drop_replica_records: list[DiaSourceReplica] = []
    keep_reassign_records: list[SourceReassignRecord] = []
    drop_reassign_records: list[SourceReassignRecord] = []

    def merge(self, other: RecordUpdates) -> RecordUpdates:
        return RecordUpdates(
            keep_initial_records=self.keep_initial_records + other.keep_initial_records,
            drop_replica_records=self.drop_replica_records + other.drop_replica_records,
            keep_reassign_records=self.keep_reassign_records + other.keep_reassign_records,
            drop_reassign_records=self.drop_reassign_records + other.drop_reassign_records,
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


def cleanup_sources(apdb_config: str, visit_detector: str, output_archive: str, update: bool) -> None:
    """Do cleanup of DiaSources in both regular and replica tables.

    Parameters
    ----------
    apdb_config : `str`
        APDB configuration location.
    visit_detector : `str`
        File produced by `find_visit_detector`.
    output_archive : `str`
        Name of the ZIP file to store CSV files with records that are deleted
        or inserted.
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
    context = apdb._context
    assert context.has_chunk_sub_partitions, "APDB replica tables must have subchunks"

    all_vd_chunks: dict[VisitDetector, list[int]] = {
        vd: sorted(chunks) for vd, chunks in VisitDetector.from_file(visit_detector)
    }
    _LOG.info("cleanup_sources: loaded %d visit-detectors", len(all_vd_chunks))

    reassign_records, close_validity_records = _read_update_records(apdb)

    reassign_chunks = sorted({r.apdb_replica_chunk for r in reassign_records})
    _LOG.debug("cleanup_sources: source reassign chunks: %s", reassign_chunks)
    close_val_chunks = sorted({r.apdb_replica_chunk for r in close_validity_records})
    _LOG.debug("cleanup_sources: closed validity chunks: %s", close_val_chunks)

    id_getter = attrgetter("diaSourceId")
    source_reassignments: dict[int, list[SourceReassignRecord]] = {
        key: sorted(items)
        for key, items in itertools.groupby(sorted(reassign_records, key=id_getter), id_getter)
    }

    # Get all replica sources for all visit/detectors/chunks.
    vd_replica_sources = _find_replica_sources(apdb, all_vd_chunks)

    # Find replica sources with more than one partition.
    replica_source_partitions = _partition_counts(
        itertools.chain.from_iterable(vd_replica_sources.values()), context.partitioner
    )
    replicas_multi_partitions = {
        source_id: len(partitions)
        for source_id, partitions in replica_source_partitions.items()
        if len(partitions) > 1
    }
    _LOG.info(
        "Number of sources with multiple partitions in replica tables: %s", len(replicas_multi_partitions)
    )
    _LOG.debug("Replica source IDs with multiple partitions: %s", sorted(replicas_multi_partitions))

    # Find all matching DiaSources in regular table.
    vd_regular_sources = _find_regular_sources(apdb, vd_replica_sources)
    assert set(vd_replica_sources) == set(vd_regular_sources), "Must have same visit/detectors"

    regular_source_partitions = _partition_counts(
        itertools.chain.from_iterable(vd_regular_sources.values()), context.partitioner
    )
    regular_multi_partitions = {
        source_id: len(partitions)
        for source_id, partitions in regular_source_partitions.items()
        if len(partitions) > 1
    }
    _LOG.info(
        "Number of sources with multiple partitions in regular tables: %s", len(replicas_multi_partitions)
    )
    _LOG.debug("Regular source IDs with multiple partitions: %s", sorted(regular_multi_partitions))

    if regular_multi_partitions != replicas_multi_partitions:
        _LOG.warning("Difference in multi-partition sources")

    archive = ZipFile(output_archive, "w", ZIP_DEFLATED, compresslevel=9)

    # Check things separately for each visit/detector, it makes it easier to
    # reason about DiaObject deduplication. Note that it is possible because
    # each visit/detector generates non-overlapping set of diaSourceIds.
    record_updates = RecordUpdates()
    for vd, replica_sources in vd_replica_sources.items():
        regular_sources = vd_regular_sources[vd]
        record_updates = record_updates.merge(
            _calc_record_updates(
                apdb._context.partitioner,
                vd,
                all_vd_chunks[vd][0],
                replica_sources,
                regular_sources,
                source_reassignments,
                close_validity_records,
            )
        )

    _LOG.info(
        "%d replica records to keep, %d to drop; %d reassign records to keep, %d to drop",
        len(record_updates.keep_initial_records),
        len(record_updates.drop_replica_records),
        len(record_updates.keep_reassign_records),
        len(record_updates.drop_reassign_records),
    )

    # Reapply DiaObject reassignments to the initial records.
    to_keep = _reassign_initial_records(
        record_updates.keep_initial_records, record_updates.keep_reassign_records
    )

    # Drop/re-create DiaSources in regular table.
    _recreate_regular_sources(
        apdb,
        to_keep,
        record_updates.drop_replica_records,
        archive,
        update,
    )

    # # Drop duplicated processing from replica table.
    _drop_replica_sources(apdb, record_updates.drop_replica_records, archive, update)

    # Drop source reassignments.
    _drop_reassignments(apdb, record_updates.drop_reassign_records, archive, update)


def _partition_counts(
    sources: Iterable[DiaSourceReplica] | Iterable[DiaSource], partitioner: Partitioner
) -> dict[int, set[int]]:
    # Find replica sources with more than one partition.
    replica_source_partitions: dict[int, set[int]] = defaultdict(set)
    for record in sources:
        apdb_part = partitioner.pixel(record.ra, record.dec)
        replica_source_partitions[record.diaSourceId].add(apdb_part)
    return replica_source_partitions


def _reassign_initial_records(
    initial_records: list[DiaSourceReplica], reassignments: list[SourceReassignRecord]
) -> list[DiaSourceReplica]:
    id_getter = attrgetter("diaSourceId")
    reassignments_by_id = {
        source_id: sorted(records)
        for source_id, records in itertools.groupby(sorted(reassignments, key=id_getter), id_getter)
    }

    result = []
    for record in initial_records:
        if assign_records := reassignments_by_id.get(record.diaSourceId):
            diaObjectId = assign_records[-1].diaObjectId
            record = record._replace(diaObjectId=diaObjectId)
        result.append(record)

    return result


def _find_replica_sources(
    apdb: ApdbCassandra, vd_chunks: dict[VisitDetector, list[int]]
) -> dict[VisitDetector, list[DiaSourceReplica]]:
    """Find full DiaSource records from replica table for a visit/detector.

    Parameters
    ----------
    apdb
        APDB instance.
    vd_chunks
        MApping of VisitDetector to the list of replica chunks.
    """
    context = apdb._context
    config = context.config

    all_chunks = frozenset(itertools.chain.from_iterable(vd_chunks.values()))

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

    vd_sources: dict[VisitDetector, list[DiaSourceReplica]] = defaultdict(list)
    source_ids: set[int] = set()
    found_chunks: set[int] = set()
    count = 0
    for success, result in results:
        if success:
            for row in result:
                row_vd = VisitDetector.from_row(row)
                chunk = cast(int, row.apdb_replica_chunk)
                if chunk in vd_chunks.get(row_vd, []):
                    diaSource = cast(DiaSourceReplica, row)
                    vd_sources[row_vd].append(diaSource)
                    source_ids.add(diaSource.diaSourceId)
                    found_chunks.add(chunk)
                    count += 1
        else:
            _LOG.error("_find_replica_sources: error returned by query: %s", result)
            raise result

    _LOG.info(
        "_find_replica_sources: "
        "found %d DiaSources with %d unique IDs in %d replica chunks for %d visit/detectors",
        count,
        len(source_ids),
        len(found_chunks),
        len(vd_sources),
    )

    return vd_sources


def _same_replica_source(
    rec1: DiaSourceReplica,
    rec2: DiaSourceReplica,
    *,
    ignore: Iterable[str] | str | None = None,
    columns: Iterable[str] | None = None,
) -> bool:
    # Compare two records ignoring difference in partitioning columns.
    if columns:
        columns = list(columns)
        dict1 = {c: getattr(rec1, c) for c in columns}
        dict2 = {c: getattr(rec2, c) for c in columns}
    else:
        dict1 = rec1._asdict()
        dict2 = rec2._asdict()

        drop_columns = ["apdb_replica_chunk", "apdb_replica_subchunk"]
        if ignore:
            if isinstance(ignore, str):
                drop_columns.append(ignore)
            else:
                drop_columns += list(ignore)
        for column in drop_columns:
            dict1.pop(column, None)
            dict2.pop(column, None)

    return dict1 == dict2


def _same_position(
    rec1: DiaSourceReplica | SourceReassignRecord,
    rec2: DiaSourceReplica | SourceReassignRecord,
) -> bool:
    # Compare two records' coordinates.
    return (rec1.ra == rec2.ra) and (rec1.dec == rec2.dec)


def _dump_records(
    replica_records: list[DiaSourceReplica],
    reassign_records: list[SourceReassignRecord],
    regular_sources: list[DiaSource],
    partitioner: Partitioner,
) -> None:
    if replica_records:
        _LOG.debug(
            "  initial replica: chunk=%d %s",
            replica_records[0].apdb_replica_chunk,
            _fmt_src_rep(replica_records[0], partitioner),
        )
        for record in replica_records[1:]:
            _LOG.debug(
                "  re-proc replica: chunk=%d %s",
                record.apdb_replica_chunk,
                _fmt_src_rep(record, partitioner),
            )
    for reassign in reassign_records:
        _LOG.debug(
            "  reassign record: chunk=%d %s",
            reassign.apdb_replica_chunk,
            reassign.as_str(partitioner),
        )
    for source in regular_sources:
        _LOG.debug("  regular  source: %s", _fmt_src(source))


def _calc_record_updates(
    partitioner: Partitioner,
    vd: VisitDetector,
    initial_chunk: int,
    replica_sources: list[DiaSourceReplica],
    regular_sources: list[DiaSource],
    source_reassignments: dict[int, list[SourceReassignRecord]],
    close_validity_records: list[CloseValidityRecord],
) -> RecordUpdates:
    """Recreate records in the regular DiaSource tables for a visit/detector.

    Parameters
    ----------
    partitioner
        Partitioner instance which can calculate partition ID from ra/dec.
    vd
        VisitDetector to which DisSources belong.
    initial_chunk
        Replica chunk for the initial processing of this visit/detector.
    replica_sources
        List of records from replica table for this VisitDetector.
    regular_sources
        List of records from regular table for this VisitDetector.
    source_reassignments
        Records of DiaSource reassignments indexed by diaSourceId.
    close_validity_records
        List of records for DiaObject validity close updates.
    """
    # For regular DiaSource table we want to have records that were created
    # in the first processing of the visit/detector and "undo" all updates
    # by further re-processing runs. This is complicated by the DiaSource
    # deduplication which was executed between re-processing runs. So in
    # addition to just re-creating records from the initial processing we need
    # to update diaObjectId of some of those initial records. This could be
    # potentially ambiguous as DiaObject deduplication used DiaSources from
    # later re-processing.
    #
    # The plan of attack:
    #  - group and order replica DiaSources according to replica chunk, the
    #    first chunk is the initial processing, the rest are re-processing;
    #    in most cases there is just on re-processing but there could be 2 or 3
    #  - find all SourceReassignRecord for all replica DiaSources in this v/d,
    #    group and order them by their replica chunk
    #  - for each re-assign chunk find earlier replica sources chunks and
    #    apply DiaObject re-assignment on those chunks, if diaSourceId exists
    #    in more than one chunk, re-assign all of them.
    #  - verify that after the last re-assignment no DiaSource from initial
    #    processing is assigned to diaSource from ``closed_dia_object_ids``
    #
    # After this the DiaSources from initial processing can be used to
    # reconstruct the content of the regular DiaSource table. Most
    # straightforward way to do this is to drop all records that match
    # ``regular_sources`` and re-create them from replica records. But it could
    # create too many tombstones which is not ideal. Instead we use a different
    # approach:
    #  - find all records in all partitions that match ``diaSourceId`` in
    #    the whole ``regular_sources``
    #  - for each ``daSourceId``:
    #    - drop record that do not match a record from the initial processing
    #    - if there are no records that match a record from the initial
    #      processing then recreate that record

    closed_dia_object_ids = {rec.diaObjectId: rec for rec in close_validity_records}

    # All diaSourceId for this visit/detector.
    all_source_ids = {source.diaSourceId for source in replica_sources}

    # Group DiaSource replica records by replica chunks.
    id_getter = attrgetter("diaSourceId")
    replicas_by_id: dict[int, list[DiaSourceReplica]] = {
        chunk: list(recs)
        for chunk, recs in itertools.groupby(sorted(replica_sources, key=id_getter), id_getter)
    }

    regular_sources_by_id: dict[int, list[DiaSource]] = {
        src_id: list(recs)
        for src_id, recs in itertools.groupby(sorted(regular_sources, key=id_getter), id_getter)
    }

    _LOG.debug("_calc_record_updates: %s, initial_chunk=%d", vd, initial_chunk)

    chunk_getter = attrgetter("apdb_replica_chunk")
    keep_initial_records: list[DiaSourceReplica] = []
    drop_replica_records: list[DiaSourceReplica] = []
    keep_reassign_records: list[SourceReassignRecord] = []
    drop_reassign_records: list[SourceReassignRecord] = []

    for source_id in sorted(all_source_ids):
        reassign_records = sorted(source_reassignments.get(source_id, []), key=chunk_getter)
        replica_records = sorted(replicas_by_id[source_id], key=chunk_getter)
        _LOG.debug(
            "_calc_record_updates: "
            "source_id=%s initial_chunk=%d replica_chunks=%s reassign_chunks=%s n_regular_sources=%d",
            source_id,
            initial_chunk,
            [r.apdb_replica_chunk for r in replica_records],
            [r.apdb_replica_chunk for r in reassign_records],
            len(regular_sources_by_id[source_id]),
        )

        # case 1 (see dm55633-notes.md)
        if replica_records[0].apdb_replica_chunk != initial_chunk:
            _LOG.debug("_calc_record_updates: #1 no initial record")
            drop_replica_records += replica_records
            drop_reassign_records += reassign_records
            continue

        # case 2
        initial_record = replica_records[0]
        if len(replica_records) == 1:
            _LOG.debug("_calc_record_updates: #2 only the initial record")
            keep_initial_records.append(initial_record)
            keep_reassign_records += reassign_records
            continue

        # cases 3-4
        if len(replica_records) > 1 and not reassign_records:
            if all(_same_replica_source(initial_record, rec) for rec in replica_records[1:]):
                _LOG.debug("_calc_record_updates: #3 no reassign, all replicas are the same")
                keep_initial_records.append(initial_record)
                drop_replica_records += replica_records[1:]
            else:
                _LOG.debug("_calc_record_updates: #4 no reassign, replicas are different")
                # DiaSources with valid DiaObjects
                valid_records = [
                    rec for rec in replica_records if rec.diaObjectId not in closed_dia_object_ids
                ]
                if valid_records:
                    keep_initial_records.append(valid_records[0])
                    drop_replica_records += [rec for rec in replica_records if rec is not valid_records[0]]
                else:
                    drop_replica_records += replica_records
            continue

        assert len(replica_records) > 1 and reassign_records, (
            "There is one or more reassign record and a few replica records"
        )

        # case 5
        if (
            len(reassign_records) == 1
            and reassign_records[0].apdb_replica_chunk < replica_records[1].apdb_replica_chunk
        ):
            _LOG.debug("_calc_record_updates: #5 single reassign for initial record")
            keep_initial_records.append(initial_record)
            drop_replica_records += replica_records[1:]
            keep_reassign_records += reassign_records
            continue

        # case 6
        if reassign_records[0].apdb_replica_chunk > replica_records[-1].apdb_replica_chunk and all(
            _same_replica_source(initial_record, rec) for rec in replica_records[1:]
        ):
            _LOG.debug("_calc_record_updates: #6 after-repro reassigns, all replicas are the same")
            keep_initial_records.append(initial_record)
            drop_replica_records += replica_records[1:]
            keep_reassign_records += reassign_records
            continue

        # cases 7-8
        if (
            all(_same_position(initial_record, rec) for rec in replica_records[1:])
            and len(reassign_records) == 1
            and reassign_records[0].apdb_replica_chunk > replica_records[-1].apdb_replica_chunk
        ):
            if reassign_records[0].diaObjectId == initial_record.diaObjectId:
                _LOG.debug("_calc_record_updates: #7 reset diaObjectId to original")
                keep_initial_records.append(initial_record)
                drop_replica_records += replica_records[1:]
                drop_reassign_records += reassign_records
            else:
                _LOG.debug("_calc_record_updates: #8 diaObjectId re-assign")
                keep_initial_records.append(initial_record)
                drop_replica_records += replica_records[1:]
                keep_reassign_records += reassign_records
            continue

        # case 9
        if initial_record.diaObjectId is None:
            if (
                len(replica_records) == 2
                and len(reassign_records) == 1
                and _same_position(initial_record, replica_records[1])
            ):
                _LOG.debug("_calc_record_updates: #9 diaObjectId is None in initial")
                keep_initial_records.append(initial_record)
                drop_replica_records += replica_records[1:]
                drop_reassign_records += reassign_records
                continue

        # case 10
        if len(replica_records) == 2 and not _same_position(initial_record, replica_records[1]):
            _LOG.debug("_calc_record_updates: #10 positions do not match")
            keep_initial_records.append(initial_record)
            drop_replica_records += replica_records[1:]
            for rr in reassign_records:
                if _same_position(rr, initial_record):
                    keep_reassign_records.append(rr)
                elif _same_position(rr, replica_records[1]):
                    drop_reassign_records.append(rr)
                else:
                    raise RuntimeError(f"Reassignment record does not match: {rr}")
            continue

        # case 11
        if (
            len(replica_records) == 2
            and _same_position(initial_record, replica_records[1])
            and len(reassign_records) == 2
            and reassign_records[0].apdb_replica_chunk > replica_records[1].apdb_replica_chunk
        ):
            _LOG.debug("_calc_record_updates: #11 two reassignments after reprocessing")
            keep_initial_records.append(initial_record)
            drop_replica_records += replica_records[1:]
            drop_reassign_records.append(reassign_records[0])
            keep_reassign_records.append(reassign_records[1])
            continue

        # case 12
        if (
            len(replica_records) == 2
            and _same_position(initial_record, replica_records[1])
            and len(reassign_records) == 2
            and reassign_records[0].apdb_replica_chunk < replica_records[1].apdb_replica_chunk
            and reassign_records[1].apdb_replica_chunk > replica_records[1].apdb_replica_chunk
        ):
            _LOG.debug("_calc_record_updates: #12 two reassignments, first before reprocessing")
            keep_initial_records.append(initial_record)
            drop_replica_records += replica_records[1:]
            keep_reassign_records.append(reassign_records[0])
            drop_reassign_records.append(reassign_records[1])
            continue

    # Check that all records that we keep have valid DiaObject.
    reassign_records_by_id = {
        key: sorted(items)
        for key, items in itertools.groupby(sorted(keep_reassign_records, key=id_getter), id_getter)
    }
    object_ids: set[int] = set()
    sources_by_object_ids: dict[int, list[DiaSourceReplica]] = defaultdict(list)
    for record in keep_initial_records:
        source_id = record.diaSourceId
        object_id = record.diaObjectId
        if reassignments := reassign_records_by_id.get(source_id):
            object_id = reassignments[-1].diaObjectId
        if object_id is not None:
            object_ids.add(object_id)
            sources_by_object_ids[object_id].append(record)
    if closed_ids := object_ids & set(closed_dia_object_ids):
        _LOG.warning("Kept records point to closed DiaObjects: %s", closed_ids)
        for closed_id in closed_ids:
            _LOG.warning("Closed ID: %s", closed_id)
            for record in sources_by_object_ids[closed_id]:
                _LOG.warning(
                    "  replica sources: chunk=%d %s",
                    record.apdb_replica_chunk,
                    _fmt_src_rep(record, partitioner),
                )

    return RecordUpdates(
        keep_initial_records=keep_initial_records,
        drop_replica_records=drop_replica_records,
        keep_reassign_records=keep_reassign_records,
        drop_reassign_records=drop_reassign_records,
    )


def _recreate_regular_sources(
    apdb: ApdbCassandra,
    to_keep: list[DiaSourceReplica],
    to_drop: list[DiaSourceReplica],
    archive: ZipFile,
    update: bool,
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
    archive
        `ZipFile` where to store CSV files with deleted or inserted records.
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

    ids_to_keep = {record.diaSourceId: record for record in to_keep}
    assert len(ids_to_keep) == len(to_keep), "All to_keep IDs must be unique"

    def _group_by_vd(records: list[DiaSourceReplica]) -> dict[VisitDetector, list[DiaSourceReplica]]:
        grouped: dict[VisitDetector, list[DiaSourceReplica]] = defaultdict(list)
        for record in to_keep:
            vd = VisitDetector(record.visit, record.detector)
            grouped[vd].append(record)
        return grouped

    _LOG.info("Searching for matching sources")
    all_matches = set(
        itertools.chain.from_iterable(_find_regular_sources(apdb, _group_by_vd(to_keep + to_drop)).values())
    )

    will_keep = []
    will_drop = []
    for record in all_matches:
        if (match := ids_to_keep.get(record.diaSourceId)) and _same_record(record, match):
            # We are going to keep this one, but remove it from ids_to_keep
            # so that we know which records we have to recreate.
            will_keep.append(record)
            del ids_to_keep[record.diaSourceId]
            _LOG.debug("_recreate_regular_sources: source_id=%d, will keep", record.diaSourceId)
        else:
            _LOG.debug("_recreate_regular_sources: source_id=%d, will drop", record.diaSourceId)
            will_drop.append(record)

    if _LOG.isEnabledFor(logging.DEBUG):
        for source_id in ids_to_keep:
            _LOG.debug("_recreate_regular_sources: source_id=%d, will recreate", source_id)

    _LOG.info(
        "Will keep %d records, drop %d records, and re-create %d records",
        len(will_keep),
        len(will_drop),
        len(ids_to_keep),
    )

    # Records in ``will_drop`` have to be deleted.
    _drop_regular_records(apdb, will_drop, archive, update)

    # Whatever is left in ids_to_keep will need to be re-created.
    _insert_regular_records(apdb, ids_to_keep.values(), archive, update)


def _same_record(regular_record: DiaSource, replica_record: DiaSourceReplica) -> bool:
    # Compare two records ignoring difference in partitioning columns.

    regular_dict = regular_record._asdict()
    del regular_dict["apdb_part"]
    replica_dict = replica_record._asdict()
    del replica_dict["apdb_replica_chunk"]
    del replica_dict["apdb_replica_subchunk"]
    return regular_dict == replica_dict


def _drop_replica_sources(
    apdb: ApdbCassandra, to_drop: list[DiaSourceReplica], archive: ZipFile, update: bool
) -> None:
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
        _LOG.info(
            "Executed %d DELETE queries on %s removing %d records", len(queries), table_name, len(to_drop)
        )
    else:
        _LOG.info(
            "Would have executed %d DELETE queries on %s removing %d records",
            len(queries),
            table_name,
            len(to_drop),
        )

    # Dump records that were deleted to CSV file.
    with archive.open("dropped-replica-records.csv", "w") as output:
        if to_drop:
            writer = csv.writer(io.TextIOWrapper(output, newline="", write_through=True))
            writer.writerow(to_drop[0]._fields)
            writer.writerows(to_drop)  # type: ignore[arg-type]


def _drop_reassignments(
    apdb: ApdbCassandra, records: list[SourceReassignRecord], archive: ZipFile, update: bool
) -> None:
    context = apdb._context
    config = context.config

    table_name = context.schema.tableName(ExtraTables.ApdbUpdateRecordChunks)
    # Primary key also includes unique_id but it is only for consistency
    # checking when replicating.
    query = (
        Delete(config.keyspace, table_name)
        .where(C("apdb_replica_chunk") == 0)
        .where(C("apdb_replica_subchunk") == 0)
        .where(C("update_time_ns") == 0)
        .where(C("update_order") == 0)
    )
    stmt = context.stmt_factory(query, prepare=True)

    queries: list[tuple[Delete, tuple]] = []
    for record in records:
        queries.append(
            (
                stmt,
                (
                    record.apdb_replica_chunk,
                    record.apdb_replica_subchunk,
                    record.update_time_ns,
                    record.update_order,
                ),
            )
        )

    if update:
        for query_chunk in chunk_iterable(queries, 1000):
            execute_concurrent(context.session, list(query_chunk), execution_profile="write")
        _LOG.info("Executed %d DELETE queries on %s", len(queries), table_name)
    else:
        _LOG.info("Would have executed %d DELETE queries on %s", len(queries), table_name)

    # Dump records that were deleted to CSV file.
    with archive.open("dropped-reassignments-records.csv", "w") as output:
        if records:
            writer = csv.writer(io.TextIOWrapper(output, newline="", write_through=True))
            writer.writerow(records[0]._fields)
            writer.writerows(records)


def _insert_regular_records(
    apdb: ApdbCassandra, records: Iterable[DiaSourceReplica], archive: ZipFile, update: bool
) -> None:
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
    with archive.open("inserted-regular-records.csv", "w") as output:
        writer = csv.writer(io.TextIOWrapper(output, newline="", write_through=True))
        writer.writerow(["apdb_time_part", "apdb_part"] + columns)

        for record in records:
            time_partition = partitioner.time_partition(
                Time(record.midpointMjdTai, format="mjd", scale="tai")
            )
            apdb_part = partitioner.pixel(record.ra, record.dec)
            values = [apdb_part] + [getattr(record, column) for column in columns]
            queries.append((statements[time_partition], values))
            writer.writerow([time_partition] + values)

    if update:
        for query_chunk in chunk_iterable(queries, 1000):
            execute_concurrent(context.session, list(query_chunk), execution_profile="write")
        _LOG.info("Inserted %d records into DiaSource", len(queries))
    else:
        _LOG.info("Would have inserted %d records into DiaSource", len(queries))


def _drop_regular_records(
    apdb: ApdbCassandra, records: list[DiaSource], archive: ZipFile, update: bool
) -> None:
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
    with archive.open("dropped-regular-records.csv", "w") as output:
        writer = csv.writer(io.TextIOWrapper(output, newline="", write_through=True))
        if records:
            writer.writerow(["apdb_time_part"] + list(records[0]._fields))

        for record in records:
            time_partition = partitioner.time_partition(
                Time(record.midpointMjdTai, format="mjd", scale="tai")
            )
            queries.append((statements[time_partition], (record.apdb_part, record.diaSourceId)))
            writer.writerow([time_partition] + list(record))  # type: ignore[call-overload]

    if update:
        for query_chunk in chunk_iterable(queries, 1000):
            execute_concurrent(context.session, list(query_chunk), execution_profile="write")
        _LOG.info("Dropped %d records from DiaSource", len(queries))
    else:
        _LOG.info("Would have dropped %d records from DiaSource", len(queries))


def _find_regular_sources(
    apdb: ApdbCassandra, vd_sources: dict[VisitDetector, list[DiaSourceReplica]]
) -> dict[VisitDetector, list[DiaSource]]:
    # Find matching DiaSources in regular table.
    vd_source_ids: dict[VisitDetector, set[int]] = defaultdict(set)
    ra_decs = set()
    midpoint_min = 100_000.0
    midpoint_max = 0.0
    for vd, sources in vd_sources.items():
        for record in sources:
            vd_source_ids[vd].add(record.diaSourceId)
            ra_decs.add((record.ra, record.dec))
            if record.midpointMjdTai < midpoint_min:
                midpoint_min = record.midpointMjdTai
            if record.midpointMjdTai > midpoint_max:
                midpoint_max = record.midpointMjdTai

    context = apdb._context
    partitioner = context.partitioner

    # Find all spatial partitions.
    pixels = {partitioner.pixel(ra, dec) for ra, dec in ra_decs}
    _LOG.info("_find_regular_sources: found %d spatial pixels", len(pixels))

    time_part_start = partitioner.time_partition(Time(midpoint_min, format="mjd", scale="tai"))
    time_part_end = partitioner.time_partition(Time(midpoint_max, format="mjd", scale="tai"))
    time_partitions = list(range(time_part_start, time_part_end + 1))
    _LOG.info("_find_regular_sources: time partitions %s", time_partitions)

    vd_regular_sources: dict[VisitDetector, list[DiaSource]] = defaultdict(list)
    record_ids: set[int] = set()
    count = 0
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
                for row in result:
                    source = cast(DiaSource, row)
                    row_vd = VisitDetector.from_row(row)
                    if source.diaSourceId in vd_source_ids.get(row_vd, set()):
                        vd_regular_sources[row_vd].append(source)
                        record_ids.add(source.diaSourceId)
                        count += 1
            else:
                _LOG.error("_find_regular_sources: error returned by query: %s", result)
                raise result

    _LOG.info(
        "_find_regular_sources: found %d DiaSource records from %d unique sources for %d visit/detectors",
        count,
        len(record_ids),
        len(vd_sources),
    )

    return vd_regular_sources


def _read_update_records(apdb: ApdbCassandra) -> tuple[list[SourceReassignRecord], list[CloseValidityRecord]]:
    context = apdb._context
    config = context.config

    # Get the list of chunks.
    chunks = apdb.get_replica().getReplicaChunks() or []
    _LOG.info("_read_update_records: found %d replica chunks", len(chunks))
    if not chunks:
        return [], []

    table_name = context.schema.tableName(ExtraTables.ApdbUpdateRecordChunks)
    query = (
        Select(config.keyspace, table_name, ["*"])
        .where(C("apdb_replica_chunk") == 0)
        .where(C("apdb_replica_subchunk") == 0)
    )
    statement = context.stmt_factory(query, prepare=True)

    queries: list[tuple] = []
    for chunk in chunks:
        for subchunk in range(config.replica_sub_chunk_count):
            queries.append((statement, (chunk.id, subchunk)))

    results = cassandra.concurrent.execute_concurrent(
        context.session,
        queries,
        results_generator=True,
        raise_on_first_error=False,
        concurrency=config.connection_config.read_concurrency,
        execution_profile="read_named_tuples",
    )

    reassign_records = []
    close_validity_records = []
    for success, result in results:
        if success:
            for row in result:
                if reassign_record := SourceReassignRecord.from_row(row):
                    reassign_records.append(reassign_record)
                elif close_validity_record := CloseValidityRecord.from_row(row):
                    close_validity_records.append(close_validity_record)
        else:
            _LOG.error("_read_update_records: error returned by query: %s", result)
            raise result

    _LOG.info(
        "_read_update_records: found %d DiaSource reassign records and %d DiaObject close validity records",
        len(reassign_records),
        len(close_validity_records),
    )

    return reassign_records, close_validity_records
