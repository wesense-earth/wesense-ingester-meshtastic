"""Tests for region_config.region_password. Run: python3 -m unittest test_region_config -v"""

import json
import os
import unittest
from unittest import mock

from region_config import region_password


class RegionPassword(unittest.TestCase):
    def test_literal_password_is_returned(self):
        self.assertEqual(region_password("ANZ", {"password": "public"}), "public")

    def test_missing_password_is_empty_string(self):
        self.assertEqual(region_password("ANZ", {}), "")

    def test_password_is_read_from_the_named_variable(self):
        with mock.patch.dict(os.environ, {"MY_BROKER_PW": "s3cret"}):
            self.assertEqual(region_password("LOCAL", {"password_env": "MY_BROKER_PW"}), "s3cret")

    def test_named_variable_wins_over_a_literal(self):
        with mock.patch.dict(os.environ, {"MY_BROKER_PW": "from-env"}):
            config = {"password": "literal", "password_env": "MY_BROKER_PW"}
            self.assertEqual(region_password("LOCAL", config), "from-env")

    def test_unset_variable_is_an_error_not_an_empty_password(self):
        with mock.patch.dict(os.environ, {}, clear=True):
            with self.assertRaises(RuntimeError) as ctx:
                region_password("LOCAL", {"password_env": "MY_BROKER_PW"})
        self.assertIn("MY_BROKER_PW", str(ctx.exception))

    def test_empty_variable_is_an_error(self):
        with mock.patch.dict(os.environ, {"MY_BROKER_PW": ""}):
            with self.assertRaises(RuntimeError):
                region_password("LOCAL", {"password_env": "MY_BROKER_PW"})


class CommittedConfig(unittest.TestCase):
    """The committed region files must not carry a private broker's password."""

    def test_local_region_uses_an_environment_variable(self):
        for path in ("config/mqtt_regions.json", "config/mqtt_regions.json.sample"):
            with open(path) as f:
                local = json.load(f)["LOCAL"]
            self.assertNotIn("password", local, f"{path}: LOCAL must not hold a literal password")
            self.assertTrue(local.get("password_env"), f"{path}: LOCAL needs password_env")


if __name__ == "__main__":
    unittest.main()
