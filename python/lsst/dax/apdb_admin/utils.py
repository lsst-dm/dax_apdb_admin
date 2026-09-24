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

__all__ = ["filter_region"]

import re
from collections.abc import Mapping
from typing import TYPE_CHECKING, Any

import lsst.sphgeom as sphgeom
from lsst.dax.apdb.pixelization import Pixelization

if TYPE_CHECKING:
    import pandas

_PIXEL_RE = re.compile(r"(\w+):(\d+)\[(\d+)\]")


def filter_region(objects: pandas.DataFrame, region: sphgeom.Region) -> pandas.DataFrame:
    """Filter out objects from a catalog which are outside given region.

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


def parse_pixel(pixel_spec: str) -> tuple[Pixelization, int]:
    """Parse pixel specification string.

    Parameters
    ----------
    pixel_spec : `str`
        Pixel specification, e.g. MQ3C:3[640].

    Returns
    -------
    pixelator : `Pixelization`
        Instance of `Pixelization`.
    pixel_index : `int`
        Index of the pixel.
    """
    if match := _PIXEL_RE.fullmatch(pixel_spec):
        pix_type = match.group(1).lower()
        level = int(match.group(2))
        pixel = int(match.group(3))
        return (Pixelization(pix_type, level, 1_000_000_000), pixel)
    else:
        raise TypeError(f"Could not parse pixel specification {pixel_spec}")
