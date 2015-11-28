# DataFrame FIT parser tests

import time
import unittest
from fit_parser import RecordDecl
from fit_parser_dataframe import parse_fit_as_dataframe

class FitPandaParserTestMethods(unittest.TestCase):
    
    def test_read_large_fit_file(self):
        filename = "large_file.fit"
        start_time = time.time()
        
        fit = parse_fit_as_dataframe(filename, [RecordDecl.time_stamp, \
                                                RecordDecl.power, \
                                                RecordDecl.heart_rate, \
                                                RecordDecl.cadence])
        
        end_time = time.time()
        print("Elapsed time was %g seconds" % (end_time - start_time))

if __name__ == '__main__':
    unittest.main()