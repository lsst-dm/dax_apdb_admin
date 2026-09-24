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

from lsst.dax.apdb_admin import utils
from lsst.sphgeom import Mq3cPixelization, Q3cPixelization


class UtilsTestCase(unittest.TestCase):
    """A test case for ``utils`` module."""

    def test_parse_pixel(self) -> None:
        """Test 'parse_pixel' method."""
        pixelization, pixel_id = utils.parse_pixel("MQ3C:3[512]")
        self.assertIsInstance(pixelization.pixelator, Mq3cPixelization)
        self.assertEqual(pixelization.level, 3)
        self.assertEqual(pixel_id, 512)

        pixelization, pixel_id = utils.parse_pixel("q3c:0[0]")
        self.assertIsInstance(pixelization.pixelator, Q3cPixelization)
        self.assertEqual(pixelization.level, 0)
        self.assertEqual(pixel_id, 0)

        with self.assertRaisesRegex(ValueError, "unknown pixelization: none"):
            utils.parse_pixel("none:0[0]")

        with self.assertRaisesRegex(TypeError, "Could not parse pixel specification"):
            utils.parse_pixel("mq3c")


if __name__ == "__main__":
    unittest.main()
