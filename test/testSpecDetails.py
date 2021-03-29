# Standard imports
import unittest
import unittest.mock as um
import sys
import os
import arrow
import logging

# Our imports
import emeval.input.spec_details as eisd

class TestSpecDetails(unittest.TestCase):
  def setUp(self):
    pass

  @um.patch.multiple('emeval.input.spec_details.SpecDetails',
                     get_current_spec = um.DEFAULT,
                     populate_spec_details = um.DEFAULT)
  def testCreation(self, get_current_spec, populate_spec_details):
        testsd = eisd.SpecDetails("foo", "bar", "baz")
        get_current_spec.assert_called_once()
        populate_spec_details.assert_called_once()

if __name__ == '__main__':
    etc.configLogging()
    unittest.main()
