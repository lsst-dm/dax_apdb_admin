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
from collections import defaultdict
from collections.abc import Iterator
from typing import NamedTuple, cast

import cassandra.concurrent
from astropy.time import Time

from lsst.daf.butler import Butler, CollectionType
from lsst.dax.apdb import Apdb, ApdbTables
from lsst.dax.apdb.cassandra import ApdbCassandra
from lsst.dax.apdb.cassandra.apdbCassandraSchema import ExtraTables
from lsst.dax.apdb.cassandra.cassandra_utils import select_concurrent
from lsst.dax.apdb.cassandra.queries import Column as C  # noqa: N817
from lsst.dax.apdb.cassandra.queries import Select
from lsst.sphgeom import Angle, UnitVector3d
from lsst.utils.iteration import chunk_iterable

_LOG = logging.getLogger(__name__)


class VisitDetector(NamedTuple):
    visit: int
    detector: int

    @classmethod
    def from_file(cls, path: str) -> Iterator[tuple[VisitDetector, tuple[int, ...]]]:
        with open(path, newline="") as file:
            for line in file:
                words = line.strip().split()
                yield (VisitDetector(int(words[0]), int(words[1])), tuple(sorted(int(w) for w in words[2:])))


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

    @classmethod
    def from_csv(cls, path: str) -> Iterator[SourceRecord]:
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

    @classmethod
    def from_csv(cls, path: str) -> Iterator[ReplicaSourceRecord]:
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


class ReplicaObjectRecord(NamedTuple):
    diaObjectId: int
    validityStartMjdTai: float
    apdb_replica_chunk: int
    apdb_replica_subchunk: int
    ra: float
    dec: float

    @classmethod
    def from_csv(cls, path: str) -> Iterator[ReplicaObjectRecord]:
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

    # First run query that only finds duplicated diaSourceIds.
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
        list[tuple[int, int, int]],
        select_concurrent(
            context.session,
            queries,
            "read_tuples",
            config.connection_config.read_concurrency,
        ),
    )
    vd_map: dict[VisitDetector, set[int]] = defaultdict(set)
    for row in records:
        vd_map[VisitDetector(*row[:2])].add(row[2])

    # Dump entries with more than one processing.
    vds = [(vd, chunks) for vd, chunks in vd_map.items() if len(chunks) > 1]
    visits = {vd.visit for vd, _ in vds}
    _LOG.info("Found %d visit-detectors from %d visits", len(vds), len(visits))
    for vd, vd_chunks in sorted(vds):
        fmt_chunks = " ".join(str(chunk) for chunk in sorted(vd_chunks))
        print(f"{vd.visit} {vd.detector:3d} {fmt_chunks}")


def find_visit_detector_butler(
    butler_pp: str, butler_daytime: str, collections_pp: str, collections_daytime: str
) -> None:
    """Find visit-detector combinations that were processed more than once.

    Parameters
    ----------
    butler_pp : `str`
        Butler with PP outputs.
    butler_daytime : `str`
        Butler with daytime outputs.
    collections_pp : `str`
        Pattern for collection names in PP butler.
    collections_daytime : `str`
        Pattern for collection names in daytime butler.
    """
    butler = Butler.from_config(butler_pp)
    collections = butler.collections.query(collections_pp, collection_types=CollectionType.RUN)
    _LOG.info("Found %d collections in PP Butler", len(collections))

    refs = butler.query_datasets("dia_source_apdb", collections=collections, instrument="LSSTCam", limit=None)
    _LOG.info("Found %d datasets in PP Butler", len(refs))

    vd_map: dict[VisitDetector, list[str]] = defaultdict(list)
    for ref in refs:
        date = ref.run.split("/")[2].partition("-")[2]
        vd_map[VisitDetector(cast(int, ref.dataId["visit"]), cast(int, ref.dataId["detector"]))].append(date)

    butler = Butler.from_config(butler_daytime)
    collections = butler.collections.query(collections_daytime, collection_types=CollectionType.RUN)
    _LOG.info("Found %d collections in daytime Butler", len(collections))

    refs = butler.query_datasets("dia_source_apdb", collections=collections, instrument="LSSTCam", limit=None)
    _LOG.info("Found %d datasets in daytime Butler", len(refs))

    for ref in refs:
        date = ref.run.split("/")[2].partition("-")[2]
        vd_map[VisitDetector(cast(int, ref.dataId["visit"]), cast(int, ref.dataId["detector"]))].append(date)

    # Dump entries with more than one processing.
    vds = [(vd, dates) for vd, dates in vd_map.items() if len(dates) > 1]
    visits = {vd.visit for vd, _ in vds}
    _LOG.info("Found %d visit-detectors from %d visits", len(vds), len(visits))
    for vd, dates in sorted(vds):
        fmt_dates = " ".join(str(date) for date in sorted(dates))
        print(f"{vd.visit} {vd.detector:3d} {fmt_dates}")


def sources_to_delete(apdb_config: str, visit_detector: str) -> None:
    """Find DiaSources to be deleted.

    Parameters
    ----------
    apdb_config : `str`
        APDB configuration location.
    visit_detector : `str`
        Path to visit-detector file produced by `find_visit_detector`.
    """
    vd_data: dict[VisitDetector, list[int]] = {}
    for vd, chunks in VisitDetector.from_file(visit_detector):
        # Do not delete DiaSources from earliest chunk.
        vd_data[vd] = sorted(chunks)[1:]
    _LOG.info("Loaded %d visit-detectors", len(vd_data))

    _find_sources(apdb_config, vd_data)


def sources_to_keep(apdb_config: str, visit_detector: str) -> None:
    """Find DiaSources to keep.

    Parameters
    ----------
    apdb_config : `str`
        APDB configuration location.
    visit_detector : `str`
        Path to visit-detector file produced by `find_visit_detector`.
    """
    vd_data: dict[VisitDetector, list[int]] = {}
    for vd, chunks in VisitDetector.from_file(visit_detector):
        # Do not delete DiaSources from earliest chunk.
        vd_data[vd] = [chunks[0]]
    _LOG.info("Loaded %d visit-detectors", len(vd_data))

    _find_sources(apdb_config, vd_data)


def _find_sources(apdb_config: str, vd_data: dict[VisitDetector, list[int]]) -> None:
    # No need to instantiate Apdb, we can look at config.
    apdb = Apdb.from_uri(apdb_config)
    assert isinstance(apdb, ApdbCassandra), "Expecting Cassandra APDB"

    context = apdb._context
    assert context.has_chunk_sub_partitions, "Must have subchunks"

    config = context.config

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

    query = Select(config.keyspace, table_name, columns, extra_clause="ALLOW FILTERING")
    query = query.where(C("apdb_replica_chunk") == 0)
    query = query.where(C("apdb_replica_subchunk") == 0)
    query = query.where(C("visit") == 0)
    query = query.where(C("detector") == 0)
    statement = context.stmt_factory(query, prepare=True)

    queries: list[tuple] = []
    for vd, chunks in vd_data.items():
        for chunk in chunks:
            for subchunk in range(config.replica_sub_chunk_count):
                queries.append((statement, (chunk, subchunk, vd.visit, vd.detector)))

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
            rows.extend(ReplicaSourceRecord(*row) for row in result)
        else:
            _LOG.error("error returned by query: %s", result)
            raise result

    _LOG.info("Found %d DiaSources", len(rows))

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
    for record in ReplicaSourceRecord.from_csv(csv_file):
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


def find_replica_objects(csv_file: str, apdb_config: str) -> None:
    """Find matching DiaSources in regular table.

    Parameters
    ----------
    csv_file : `str`
        Path to CSV file produced by `sources_to_delete/keep`.
    apdb_config : `str`
        APDB configuration location.
    """
    objects_by_chunk: dict[int, set[int]] = defaultdict(set)
    source_ids = set()
    object_ids = set()
    object_chunk_ids = set()
    sources = list(ReplicaSourceRecord.from_csv(csv_file))
    for source in sources:
        if source.diaObjectId is not None:
            objects_by_chunk[source.apdb_replica_chunk].add(source.diaObjectId)
            object_ids.add(source.diaObjectId)
            object_chunk_ids.add((source.diaObjectId, source.apdb_replica_chunk))
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
    ]

    query = Select(config.keyspace, table_name, columns)
    query = query.where(C("apdb_replica_chunk") == 0)
    query = query.where(C("apdb_replica_subchunk") == 0)
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
        execution_profile="read_tuples",
    )
    rows: list[ReplicaObjectRecord] = []
    for success, result in results:
        if success:
            for row in result:
                rec = ReplicaObjectRecord(*row)
                if rec.diaObjectId in objects_by_chunk[rec.apdb_replica_chunk]:
                    rows.append(rec)
        else:
            _LOG.error("error returned by query: %s", result)
            raise result

    _LOG.info("Found %d DiaObjects", len(rows))

    # Sort it all by diaObjectId and validityStartMjdTai.
    rows.sort(key=lambda r: (r.diaObjectId, r.validityStartMjdTai))

    # Dump everything.
    writer = csv.writer(sys.stdout)
    writer.writerow(columns)
    writer.writerows(rows)
