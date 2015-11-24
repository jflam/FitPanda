from crc import compute_crc
import io
import unittest

class Crc16TestMethods(unittest.TestCase):

    def test_simple_strings(self):
        strings = [
                b"123456789",
                b"0123456789",
                b"01234567890",
                b"012345678901",
                b"0123456789012",
                b"01234567890123",
                b"012345678901234",
                b"0123456789012345",
                b"01234567890123456"
                ]

        crcs = [
                0xbb3d,
                0x443d,
                0xc585,
                0x77c5,
                0x8636,
                0x0346,
                0x2583,
                0xb6a4,
                0xad37
                ]

        self.assertEqual(len(strings), len(crcs))

        for i in range(len(strings)):
            s = strings[i]
            crc = compute_crc(io.BytesIO(s), len(s))
            self.assertEqual(crc, crcs[i])

if __name__ == '__main__':
    unittest.main()

# Test
def foo():
    import io
    import os
    filename = "C:\\Users\\jflam\\OneDrive\\Garmin\\2010-03-22-07-25-44.fit"
    #filename = "C:\\Users\\jflam\\OneDrive\\Garmin\\2010-03-21-20-31-06.fit"
    stream = io.open(filename, "rb")
    crc = compute_crc(stream, os.path.getsize(filename))
    print(crc)

