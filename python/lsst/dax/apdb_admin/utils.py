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


from collections.abc import Mapping
from typing import TYPE_CHECKING, Any

from lsst import sphgeom

if TYPE_CHECKING:
    import pandas


def filter_region(objects: pandas.DataFrame, region: sphgeom.Region) -> pandas.DataFrame:
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
