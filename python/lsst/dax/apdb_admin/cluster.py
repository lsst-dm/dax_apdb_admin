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

__all__ = ["cluster_by_xy"]

import itertools
import logging
import math
from collections.abc import Iterable
from typing import Protocol

_LOG = logging.getLogger(__name__)


class ObjectWithXY(Protocol):
    x: float
    y: float


def cluster_by_xy[T: ObjectWithXY](
    objects: Iterable[T], max_dist: float = 0.1
) -> dict[tuple[float, float], list[T]]:
    """Cluster object by position.

    Parameters
    ----------
    objects : `Iterable` [`ObjectWithXY`]
        Collection of objects wit x/y coordinates.
    max_dist : `float`
        Max distance for object considered the same.

    Returns
    -------
    objects
        Objects grouped by close coordinates.
    """
    max_dist_sq = max_dist**2
    obj_list = list(objects)
    n_objects = len(obj_list)

    result = {}
    while obj_list:
        obj = obj_list.pop()
        non_group: list[T] = []
        group: list[T] = []
        for other in obj_list:
            dist_sq = (obj.x - other.x) ** 2 + (obj.y - other.y) ** 2
            if dist_sq < max_dist_sq:
                group.append(other)
            else:
                non_group.append(other)
        group.append(obj)
        x = sum(obj.x for obj in group) / len(group)
        y = sum(obj.y for obj in group) / len(group)
        result[(x, y)] = group

        obj_list = non_group

    if _LOG.isEnabledFor(logging.DEBUG):
        min_cluster_distance = 1e10
        clusters = list(result)
        for c1, c2 in itertools.combinations(clusters, 2):
            min_cluster_distance = min(min_cluster_distance, math.dist(c1, c2))
        avg_cluster_size = n_objects / len(clusters)

        _LOG.debug(
            "Found %d clusters, min. distance between clusters: %.5f, avg. cluster size: %.2f",
            len(result),
            min_cluster_distance,
            avg_cluster_size,
        )

    return result
