# Fast FIT parser written in Python
# TODO: cythonize it

import io
import struct
from crc import compute_crc

# Internal helper methods
def read_byte(stream):
    return int.from_bytes(stream.read(1), byteorder = "little", signed = False)

def read_int16(stream):
    return int.from_bytes(stream.read(2), byteorder = "little", signed = True)

def read_uint16(stream):
    return int.from_bytes(stream.read(2), byteorder = "little", signed = False)

def read_uint32(stream):
    return int.from_bytes(stream.read(4), byteorder = "little", signed = False)

class FileHeader:
    def __init__(self, stream):
        self.stream = stream
        self.Size = read_byte(stream)
        self.ProtocolVersion = read_byte(stream)
        self.ProfileVersion = read_int16(stream)
        self.DataSize = read_uint32(stream)

        # Assert .fit file signature
        b1 = stream.read(1)
        b2 = stream.read(1)
        b3 = stream.read(1)
        b4 = stream.read(1)

        assert b1 == b'.' and b2 == b'F' and b3 == b'I' and b4 == b'T'

        # Read optional CRC
        if self.Size > 12:
            self.CRC = int.from_bytes(stream.read(2), byteorder = "little")

class FieldDefinition:
    def __init__(self, stream, current_offset):
        self.FieldDefinitionNumber = read_byte(stream)
        self.FieldSize = read_byte(stream)
        self.FieldOffset = current_offset
        self.FieldType = read_byte(stream)

class MessageDefinition:
    def __init__(self, header, stream):
        reserved = read_byte(stream)

        # 0 == Definition and Data messages are little-endian
        # 1 == Definition and Data messages are big-endian

        # TODO: actually do something with this flag. right now everything
        # that we see is little-endian

        self.architecture = read_byte(stream)

        self.GlobalMessageNumber = read_uint16(stream)

        self.FieldDefinitions = []
        field_count = read_byte(stream)
        current_offset = 0

        for i in range(field_count):
            field_definition = FieldDefinition(stream, current_offset)
            self.FieldDefinitions.append(field_definition)
            current_offset += field_definition.FieldSize

        self.Size = current_offset

    def MessageDefinitionSize(self):
        return len(self.FieldDefinitions) * 3 + 5

class Message:
    def __init__(self, header, message_definition, stream):
        self.header = header
        self.message_definition = message_definition
        self.message_data = stream.read(message_definition.Size)
        self.is_initialized = False

    # Linear search through a Message's FieldDefinitions 
    # If found, will also guarantee that the internal BinaryReader
    # over the Message is initialized, and pointing at the start
    # of the field.
    # Returns null if not found.
    def _get_field_definition(self, field_number):
        for field_definition in self.message_definition.FieldDefinitions:
            if field_definition.FieldDefinitionNumber == field_number:
                if not self.is_initialized:
                    self.stream = io.BytesIO(self.message_data)
                    self.is_initialized = True
                self.stream.seek(field_definition.FieldOffset)
                return field_definition
        return None
                    
# Function that parses a fit file. Note that this is a generator.
def parse_fit_file(path):
    stream = io.open(path, "rb")
    file_header = FileHeader(stream)
    pos = stream.seek(0)
    crc = compute_crc(stream, file_header.Size + file_header.DataSize)
    file_crc = read_uint16(stream)

    # TODO: debug why CRC is failing with real files
    # print(pos, format(self.crc, "0x"), format(self.file_crc, "0x"))

    # Seek to the start of the data (past the file header)
    pos = stream.seek(file_header.Size)

    bytes_to_read = file_header.DataSize
    bytes_read = 0

    # Message definitions are parsed internally by the parser and not exposed to
    # the caller. We store all of them in this dict:
    local_message_definitions = {}

    while bytes_read < bytes_to_read:
        header = read_byte(stream)

        # Normal header (vs. timestamp offset header is indicated by bit 7)
        # Message type is indicated by bit 6 
        #   1 == definition
        #   0 == record
        local_message_number = header & 0xf

        if (header & 0x80) == 0 and (header & 0x40) == 0x40:

            # Parse the message definition and store the definition in our array
            message_definition = MessageDefinition(header, stream)
            local_message_definitions[local_message_number] = message_definition
            bytes_read += message_definition.MessageDefinitionSize() + 1

        elif (header & 0x80) == 0 and (header & 0x40) == 0:
            current_message_definition = local_message_definitions[local_message_number]
            assert current_message_definition is not None

            # This design reads the current message into an in-memory byte array.
            # An alternate design would involve passing in the current binary reader
            # and allowing the caller of Message to read fields using the binary
            # reader directly instead of creating a MemoryStream over the byte array
            # and using a different BinaryReader in the Message. I have done 
            # exactly this and measured the performance, and it is actually SLOWER
            # than this approach. I haven't root caused why, but would assume that
            # Seek-ing arbitrarily using the BinaryReader over the FileStream is 
            # slow vs. Seek-ing over a BinaryReader over a MemoryStream.

            message = Message(header, current_message_definition, stream)
            yield message 

            bytes_read += current_message_definition.Size + 1


# Simple test harness
# filename = "c:\\users\\jflam\\onedrive\\garmin\\2010-03-22-07-25-44.fit"
filename = "large_file.fit"

messages = []
for message in parse_fit_file(filename):
    # TODO: stuff things into a data frame
    messages.append(message)

print(len(messages))