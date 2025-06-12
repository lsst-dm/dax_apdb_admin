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

__all__ = ["dump_visit"]

import logging
from collections import Counter
from collections.abc import Collection, Mapping
from typing import Any

import pandas
from astropy.time import Time

from lsst import sphgeom
from lsst.daf.butler import Butler
from lsst.dax.apdb import Apdb

from .. import model

_LOG = logging.getLogger(__name__)


def _filter_region(objects: pandas.DataFrame, region: sphgeom.Region) -> pandas.DataFrame:
    """Filter out objects from a catalog which are outside region.

    Parameters
    ----------
    objects : `pandas.DataFrame`
        Catalog containing DiaObject records.
    region : `sphgeom.Region`
        Region to filter records to.

    Returns
    -------
    dataframe : `pandas.DataFrame`
        Filtered DataFrame with records contained in the region.
    """
    if objects.empty:
        return objects

    def in_region(obj: Mapping[str, Any]) -> bool:
        lonLat = sphgeom.LonLat.fromDegrees(obj["ra"], obj["dec"])
        dir_obj = sphgeom.UnitVector3d(lonLat)
        return region.contains(dir_obj)

    mask = objects.apply(in_region, axis=1, result_type="reduce")
    return objects[mask]


def dump_visit(
    butler_config: str,
    apdb_config: str,
    instrument: str,
    visit: int,
    detectors: Collection[int],
    verbose: int,
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
    verbose : `int`
        Verbosity level.
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
    first_visit_count: Counter = Counter()
    for record in region_records:
        if detectors and record.detector not in detectors:
            continue

        print(f"--- Processing visit {record.visit} detector {record.detector}")
        region = record.region

        # Get objects.sources from the region.
        objects_df = apdb.getDiaObjects(region)
        objects_df = _filter_region(objects_df, region)
        object_ids = set(objects_df["diaObjectId"])
        print(f"Found {len(object_ids)} DiaObjects")

        sources_df = apdb.getDiaSources(region, object_ids, visit_time)
        assert sources_df is not None
        print(f"Found {len(sources_df)} DiaSources")

        forced_sources_df = apdb.getDiaForcedSources(region, object_ids, visit_time)
        assert forced_sources_df is not None
        print(f"Found {len(forced_sources_df)} DiaForcedSources")

        source_infos = model.SourceInfo.from_pandas(sources_df)
        source_groups = model.SourceInfo.group_by_object(source_infos)

        # Find all objects that were first found in this visit.
        new_source_groups = {
            oid: source_group for oid, source_group in source_groups.items() if source_group[0].visit == visit
        }
        print(f"{len(new_source_groups)} new DiaObjects in this visit/detector")

        if verbose > 0:
            # Dump everything.
            object_infos = model.ObjectInfo.from_pandas(objects_df)

            for oinfo in object_infos:
                first_visit = None
                if oinfo.diaObjectId in source_groups:
                    first_visit = source_groups[oinfo.diaObjectId][0].visit
                    first_visit_count[first_visit] += 1
                print(
                    f"   DiaObject: diaObjectId={oinfo.diaObjectId} nDiaSources={oinfo.nDiaSources} "
                    f"ra={oinfo.ra} dec={oinfo.dec} first_visit={first_visit}"
                )
                if verbose > 1:
                    for sinfo in source_groups.get(oinfo.diaObjectId, []):
                        print(
                            f"      DiaSource: diaSourceId={sinfo.diaSourceId} "
                            f"visit={sinfo.visit} detector={sinfo.detector} "
                            f"time_processed={sinfo.time_processed} "
                            f"midpointMjdTai={sinfo.midpointMjdTai} "
                            f"ra={oinfo.ra} dec={oinfo.dec}"
                        )

                if verbose > 2:
                    # Dump forced sources.
                    forced_sources_infos = model.ForcedSourceInfo.from_pandas(forced_sources_df)
                    forced_sources_groups = model.ForcedSourceInfo.group_by_object(forced_sources_infos)
                    for fsinfo in forced_sources_groups.get(oinfo.diaObjectId, []):
                        print(
                            f"      DiaForcedSource: diaForcedSourceId={fsinfo.diaForcedSourceId} "
                            f"visit={fsinfo.visit} detector={fsinfo.detector} "
                            f"time_processed={fsinfo.time_processed} "
                            f"midpointMjdTai={fsinfo.midpointMjdTai} "
                            f"ra={oinfo.ra} dec={oinfo.dec}"
                        )

    if first_visit_count:
        print("   First visit counts (all detectors):")
        for visit, count in sorted(first_visit_count.items()):
            print(f"      {visit}: {count:6d}")
