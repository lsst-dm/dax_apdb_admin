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

__all__ = ["delete_visit"]

import logging
from collections.abc import Collection

from astropy.time import Time

from lsst.daf.butler import Butler
from lsst.dax.apdb import Apdb
from lsst.dax.apdb.apdbAdmin import DiaForcedSourceLocator, DiaObjectLocator, DiaSourceLocator

from .. import model, utils

_LOG = logging.getLogger(__name__)


def delete_visit(
    butler_config: str,
    apdb_config: str,
    instrument: str,
    visit: int,
    detectors: Collection[int],
    delete: bool,
    no_sources: bool,
) -> None:
    """List contents of APDB index file.

    Parameters
    ----------
    butler_config : `str`
        Butler configuration location.
    apdb_config : `str`
        APDB configuration location.
    instrument : `str`
        Instrument name.
    visit : `int`
        Visit number.
    detectors : `~collections.abc.Collection` [`int`]
        List of detector numbers, if empty then all SCIENCE detectors are used.
    delete : `bool`
        If `True` then do actual deletion, othervise just print records to be
        deleted.
    no_sources : `bool`
        If `True` only delete objects that have no associated sources, and
        delete associated forced sources.
    """
    # make sorted list of region records
    butler = Butler.from_config(butler_config)

    # Only look at the SCIENCE detectors
    detector_records = butler.query_dimension_records("detector", instrument=instrument, visit=visit)
    science_detectors = {detector.id for detector in detector_records if detector.purpose == "SCIENCE"}
    detectors = set(detectors)
    if detectors:
        unknown = detectors - science_detectors
        if unknown:
            _LOG.warning("Specified detectors are not known in this visit: %s", unknown)
        detectors &= science_detectors
    else:
        detectors = science_detectors

    region_records = butler.query_dimension_records(
        "visit_detector_region", instrument=instrument, visit=visit
    )
    region_records = sorted(region_records, key=lambda record: record.detector)

    apdb = Apdb.from_uri(apdb_config)

    visit_time = Time.now()
    for record in region_records:
        if detectors and record.detector not in detectors:
            continue

        _LOG.info(f"--- Processing visit {record.visit} detector {record.detector}")
        region = record.region

        # Get objects.sources from the region.
        objects_df = apdb.getDiaObjects(region)
        objects_df = utils.filter_region(objects_df, region)
        object_ids = set(objects_df["diaObjectId"])
        _LOG.info(f"Found {len(object_ids)} DiaObjects")

        sources_df = apdb.getDiaSources(region, object_ids, visit_time)
        assert sources_df is not None
        _LOG.info(f"Found {len(sources_df)} DiaSources")

        forced_sources_df = apdb.getDiaForcedSources(region, object_ids, visit_time)
        assert forced_sources_df is not None
        _LOG.info(f"Found {len(forced_sources_df)} DiaForcedSources")

        object_infos = model.ObjectInfo.from_pandas(objects_df)
        object_map = {obj.diaObjectId: obj for obj in object_infos}

        source_infos = model.SourceInfo.from_pandas(sources_df)
        source_groups = model.SourceInfo.group_by_object(source_infos)

        forced_sources_infos = model.ForcedSourceInfo.from_pandas(forced_sources_df)
        forced_sources_groups = model.ForcedSourceInfo.group_by_object(forced_sources_infos)

        # Find all objects that were first found in this visit.
        if no_sources:
            object_ids_to_delete = {oid for oid in object_ids if oid not in source_groups}
            _LOG.info(f"{len(object_ids_to_delete)} no-source DiaObjects in this visit/detector")
        else:
            new_source_groups = {
                oid: source_group
                for oid, source_group in source_groups.items()
                if source_group[0].visit == visit and oid in object_ids
            }
            object_ids_to_delete = set(new_source_groups)
            _LOG.info(f"{len(object_ids_to_delete)} new DiaObjects in this visit/detector")

        if not object_ids_to_delete:
            _LOG.info("Nothing to delete in this visit/detector.")
        elif not delete:
            print("DiaObjects to delete:")
            for oid in sorted(object_ids_to_delete):
                if oinfo := object_map.get(oid):
                    print(
                        f"   DiaObject: diaObjectId={oinfo.diaObjectId} nDiaSources={oinfo.nDiaSources} "
                        f"ra={oinfo.ra} dec={oinfo.dec}"
                    )
            print("DiaSources to delete:")
            for oid, sinfos in source_groups.items():
                if oid in object_ids_to_delete:
                    for sinfo in sinfos:
                        print(
                            f"   DiaSource: diaSourceId={sinfo.diaSourceId} "
                            f"visit={sinfo.visit} detector={sinfo.detector} "
                            f"time_processed={sinfo.time_processed} "
                            f"ra={sinfo.ra} dec={sinfo.dec}"
                        )
            print("ForcedDiaSources to delete:")
            for oid, fsinfos in forced_sources_groups.items():
                if oid in object_ids_to_delete:
                    for fsinfo in fsinfos:
                        print(
                            f"   DiaForcedSource: diaForcedSourceId={fsinfo.diaForcedSourceId} "
                            f"visit={fsinfo.visit} detector={fsinfo.detector} "
                            f"time_processed={fsinfo.time_processed} "
                            f"ra={fsinfo.ra} dec={fsinfo.dec}"
                        )
        else:
            object_locators = []
            for oid in object_ids_to_delete:
                if oinfo := object_map.get(oid):
                    object_locators.append(
                        DiaObjectLocator(diaObjectId=oinfo.diaObjectId, ra=oinfo.ra, dec=oinfo.dec)
                    )

            source_locators = [
                DiaSourceLocator(
                    diaSourceId=sinfo.diaSourceId,
                    diaObjectId=sinfo.diaObjectId,
                    ra=sinfo.ra,
                    dec=sinfo.dec,
                    midpointMjdTai=sinfo.midpointMjdTai,
                )
                for sinfo in source_infos
                if sinfo.diaObjectId in object_ids_to_delete
            ]

            forced_source_locators = [
                DiaForcedSourceLocator(
                    diaObjectId=fsinfo.diaObjectId,
                    visit=fsinfo.visit,
                    detector=fsinfo.detector,
                    ra=fsinfo.ra,
                    dec=fsinfo.dec,
                    midpointMjdTai=fsinfo.midpointMjdTai,
                )
                for fsinfo in forced_sources_infos
                if fsinfo.diaObjectId in object_ids_to_delete
            ]

            _LOG.info(f"Deleting {len(object_locators)} DiaObjects with associated sources")
            apdb.admin.delete_records(object_locators, source_locators, forced_source_locators)
