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

__all__ = ["set_validity_end"]

import json
import logging

from astropy.time import Time

from lsst.dax.apdb import Apdb, DiaObjectId

_LOG = logging.getLogger(__name__)


def set_validity_end(
    apdb_config: str,
    jsonl: str,
    update: bool,
    time: str | None,
) -> None:
    """Update validityEnd for DiaObjects.

    Parameters
    ----------
    apdb_config : `str`
        APDB configuration location.
    jsonl : `str`
        Name of the file with DiaObjects in JSON lines format.
    update : `bool`
        If `True` then do actual updates, otherwise just print records to be
        updated.
    time : `str`
        Time in astropy ISOT format and TAI scale to use for validityEnd.
    """
    validityEnd = Time(time, format="isot", scale="tai") if time else Time.now().tai

    dia_objects: dict[int, DiaObjectId] = {}
    count = 0
    with open(jsonl) as file:
        for line in file:
            if not line:
                continue

            dia_object_dict = json.loads(line)
            dia_object = DiaObjectId(
                diaObjectId=dia_object_dict["diaObjectId"],
                ra=dia_object_dict["ra"],
                dec=dia_object_dict["dec"],
            )
            dia_objects[dia_object.diaObjectId] = dia_object
            count += 1

    _LOG.info("Read %d DiaSource records with %d unique IDs", count, len(dia_objects))

    if update:
        _LOG.info(
            "Setting endValidityMjdTai to %s (%s) for %d DiaObjects",
            validityEnd.mjd,
            validityEnd,
            len(dia_objects),
        )
        apdb = Apdb.from_uri(apdb_config)
        apdb.setValidityEnd(
            list(dia_objects.values()),
            validityEnd,
            raise_on_missing_id=True,
        )
    else:
        _LOG.info(
            "Would set endValidityMjdTai to %s (%s) for %d DiaObjects",
            validityEnd.mjd,
            validityEnd,
            len(dia_objects),
        )
