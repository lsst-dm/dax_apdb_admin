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

__all__ = ["visit_detectors", "visit_region_records"]

import logging
from collections.abc import Collection
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from lsst.daf.butler import Butler, DimensionRecord

_LOG = logging.getLogger(__name__)


def visit_detectors(
    butler: Butler, instrument: str, visit: int, detectors: Collection[int] | None = None
) -> set[int]:
    """Return list of detector IDs for given instrument and visit.

    Parameters
    ----------
    butler : `lsst.daf.butler.Butler`
        Butler instance.
    instrument : `str`
        Instrument name.
    visit : `int`
        Visit number.
    detectors : `~collections.abc.Collection` [`int`]
        List of detector numbers, if `None` or empty then IDs of all SCIENCE
        detectors are returned. If not empty then a subset of the provided
        IDs corresponding to SCIENCE detectors is returned.

    Returns
    -------
    detectors : `set` [`int`]
        Collection of detector IDs.
    """
    # Only look at the SCIENCE detectors
    detector_records = butler.query_dimension_records("detector", instrument=instrument, visit=visit)
    science_detectors = {detector.id for detector in detector_records if detector.purpose == "SCIENCE"}
    if detectors:
        detectors = set(detectors)
        unknown = detectors - science_detectors
        if unknown:
            _LOG.warning("Specified detectors are not known in this visit: %s", unknown)
        detectors &= science_detectors
    else:
        detectors = science_detectors
    return detectors


def visit_region_records(
    butler: Butler, instrument: str, visit: int, detectors: Collection[int] | None = None
) -> list[DimensionRecord]:
    """Return list of visit/detector region dimension records for given
    instrument and visit.

    Parameters
    ----------
    butler : `lsst.daf.butler.Butler`
        Butler instance.
    instrument : `str`
        Instrument name.
    visit : `int`
        Visit number.
    detectors : `~collections.abc.Collection` [`int`]
        List of detector numbers, if `None` or empty then IDs of all SCIENCE
        detectors are used. If not empty then a subset of the provided
        IDs corresponding to SCIENCE detectors is used.

    Returns
    -------
    region : `list` [`lsst.daf.butler.DimensionRecord`]
        Collection of dimension records ordered by detector ID.
    """
    detector_ids = visit_detectors(butler, instrument, visit, detectors)

    region_records = butler.query_dimension_records(
        "visit_detector_region", instrument=instrument, visit=visit
    )
    region_records = sorted(
        (record for record in region_records if record.detector in detector_ids),
        key=lambda record: record.detector,
    )
    return region_records
