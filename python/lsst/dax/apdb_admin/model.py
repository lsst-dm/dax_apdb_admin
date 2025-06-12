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

__all__ = ["ForcedSourceInfo", "ObjectInfo", "SourceInfo"]

import datetime
from collections import defaultdict
from collections.abc import Collection
from typing import TYPE_CHECKING, NamedTuple

if TYPE_CHECKING:
    import pandas


class ObjectInfo(NamedTuple):
    """NamedTuple representing a subset of columns in DiaObject table used
    in this package.
    """

    diaObjectId: int
    ra: float  # in degrees
    dec: float  # in degrees
    nDiaSources: int

    @staticmethod
    def from_pandas(df: pandas.DataFrame) -> list[ObjectInfo]:
        infos = []
        for row in df.itertuples(index=False):
            info = ObjectInfo(
                diaObjectId=row.diaObjectId,
                ra=row.ra,
                dec=row.dec,
                nDiaSources=row.nDiaSources,
            )
            infos.append(info)
        return infos


class SourceInfo(NamedTuple):
    """NamedTuple representing a subset of columns in DiaSource table used
    in this package.
    """

    diaObjectId: int
    diaSourceId: int
    time_processed: datetime.datetime
    midpointMjdTai: float
    visit: int
    detector: int
    ra: float  # in degrees
    dec: float  # in degrees

    @staticmethod
    def from_pandas(df: pandas.DataFrame) -> list[SourceInfo]:
        """Make list of SourceInfos from pandas DataFrame.

        Parameters
        ----------
        df : `pandas.DataFrame`
            DataFrame to convert to SourceInfo collection.

        Returns
        -------
        infos : `list` [`SourceInfo`]
            Resulting SourceInfo instances.
        """
        infos = []
        for row in df.itertuples(index=False):
            source = SourceInfo(
                diaObjectId=row.diaObjectId,
                diaSourceId=row.diaSourceId,
                time_processed=row.time_processed.replace(tzinfo=datetime.UTC),
                midpointMjdTai=row.midpointMjdTai,
                visit=row.visit,
                detector=row.detector,
                ra=row.ra,
                dec=row.dec,
            )
            infos.append(source)
        return infos

    @staticmethod
    def group_by_object(infos: Collection[SourceInfo]) -> dict[int, list[SourceInfo]]:
        """Group SourceInfos by diaObjectId, ordering them in each group by
        midpointTaiMjd.

        Parameters
        ----------
        infos : `~collections.abc.Collection` [`SourceInfo`]
            Sources to group.

        Returns
        -------
        sources : `dict`
            Sources grouped bu diaObjectId.
        """
        info_map = defaultdict(list)
        for info in infos:
            info_map[info.diaObjectId].append(info)
        return {oid: sorted(infos, key=lambda x: x.midpointMjdTai) for oid, infos in info_map.items()}


class ForcedSourceInfo(NamedTuple):
    """NamedTuple representing a subset of columns in DiaForcedSource table
    used in this package.
    """

    diaObjectId: int
    diaForcedSourceId: int
    time_processed: datetime.datetime
    midpointMjdTai: float
    visit: int
    detector: int
    ra: float  # in degrees
    dec: float  # in degrees

    @staticmethod
    def from_pandas(df: pandas.DataFrame) -> list[ForcedSourceInfo]:
        """Make list of SourceInfos from pandas DataFrame.

        Parameters
        ----------
        df : `pandas.DataFrame`
            DataFrame to convert to ForcedSourceInfo collection.

        Returns
        -------
        infos : `list` [`ForcedSourceInfo`]
            Resulting ForcedSourceInfo instances.
        """
        infos = []
        for row in df.itertuples(index=False):
            info = ForcedSourceInfo(
                diaObjectId=row.diaObjectId,
                diaForcedSourceId=row.diaForcedSourceId,
                time_processed=row.time_processed.replace(tzinfo=datetime.UTC),
                midpointMjdTai=row.midpointMjdTai,
                visit=row.visit,
                detector=row.detector,
                ra=row.ra,
                dec=row.dec,
            )
            infos.append(info)

        return infos

    @staticmethod
    def group_by_object(infos: Collection[ForcedSourceInfo]) -> dict[int, list[ForcedSourceInfo]]:
        """Group ForcedSourceInfos by diaObjectId, ordering them in each group
        by midpointTaiMjd.

        Parameters
        ----------
        infos : `~collections.abc.Collection` [`ForcedSourceInfo`]
            ForcedSources to group.

        Returns
        -------
        sources : `dict`
            ForcedSources grouped bu diaObjectId.
        """
        info_map = defaultdict(list)
        for info in infos:
            info_map[info.diaObjectId].append(info)
        return {oid: sorted(infos, key=lambda x: x.midpointMjdTai) for oid, infos in info_map.items()}
