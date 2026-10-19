# This file is part of dax_apdb_admin.
#
# Developed for the LSST Data Management System.
# This product includes software developed by the LSST Project
# (http://www.lsst.org).
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
# along with this program.  If not, see <http://www.gnu.org/licenses/>.

from __future__ import annotations

import unittest

from lsst.dax.apdb_admin import cluster


class _XY:
    def __init__(self, x: float, y: float):
        self.x = x
        self.y = y


class ClusterTestCase(unittest.TestCase):
    """A test case for ``cluster`` module."""

    def test_cluster_by_xy(self) -> None:
        """Test 'cluster_by_xy' method."""
        xys = [
            _XY(x=0.0, y=0.5),
            _XY(x=0.4, y=0.0),
            _XY(x=-0.4, y=-0.5),
            _XY(x=10.0, y=0.1),
            _XY(x=11.0, y=0.5),
            _XY(x=10.0, y=10.0),
            _XY(x=1000.0, y=99.5),
            _XY(x=999.4, y=100.5),
        ]

        clusters = cluster.cluster_by_xy(xys)
        self.assertEqual(len(clusters), 4)
        self.assertEqual(set(clusters), {(0.0, 0.0), (10.5, 0.3), (10.0, 10.0), (999.7, 100.0)})
        self.assertCountEqual([len(c) for c in clusters.values()], [3, 2, 1, 2])


if __name__ == "__main__":
    unittest.main()
