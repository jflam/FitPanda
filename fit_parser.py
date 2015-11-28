# Fast FIT parser written in Python
# TODO: cythonize it

from datetime import datetime, tzinfo
import io
import struct
from enum import IntEnum

# Import cythonized fast CRC16 computation algorithm

from crc import compute_crc

# FIT file declarations. This is a whole whack of static definitions
# based on my reading of the .FIT specification. All of this makes
# it easier for the user to type in readable names in their code.
# Note that declarations != definitions. Later in this file, you will
# see definitions, which are runtime, dynamic definitions as read
# from the .FIT file being parsed. 
#
# TODO: write some examples here
# 
# A key element of the parser is how it will cleanly map a declaration 
# (a statement of what the caller wants) to a definition (metadata about 
# where that field is stored within a binary record). This lookup occurs
# at runtime, where a user provides a FieldDecl and the Message object
# will look up its internal FieldDefinition object to compute the offset
# into the record where the data is stored. The FieldDecl contains a 
# field number which is used to lookup the corresponding FieldDefinition
# within the Message object. Note that the FIT specifications contain
# *many* aliases for FieldDecls. However, the authoritative representation
# of the field is always the FieldDefinition, which is what was actually
# stored in the file. So we always do a one-way lookup from a FieldDecl
# to the FieldDefinition, never the other way around.


# Events are records that can appear anywhere within a .FIT
# file. This enumeration describes the types of valid events.

class Event(IntEnum):
    timer = 0,
    workout = 3,
    workout_step = 4,
    power_down = 5,
    power_up = 6,
    off_course = 7,
    session = 8,
    lap = 9,
    course_point = 10,
    battery = 11,
    virtual_partner_pace = 12,
    hr_high_alert = 13,
    hr_low_alert = 14,
    speed_high_alert = 15,
    speed_low_alert = 16,
    cad_high_alert = 17,
    cad_low_alert = 18,
    power_high_alert = 19,
    power_low_alert = 20,
    recovery_hr = 21,
    battery_low = 22,
    time_duration_alert = 23,
    distance_duration_alert = 24,
    calorie_duration_alert = 25,
    activity = 26,
    fitness_equipment = 27,
    length = 28,
    user_marker = 32,
    sport_point = 33,
    calibration = 36,
    invalid = 0xFF

# This enumeration describes the type of an event

class EventType(IntEnum):
    start = 0,
    stop = 1,
    consecutive_depreciated = 2,
    marker = 3,
    stop_all = 4,
    begin_depreciated = 5,
    end_depreciated = 6,
    end_all_depreciated = 7,
    stop_disable = 8,
    stop_disableAll = 9,
    invalid = 0xFF

# We need some more information than what is possible with Enums
# alone for field definitions.

# TODO: why does a FieldDecl contain the is_enum or is_array fields?
# I don't think these are valid here, which then reduces FieldDecls to
# simple enums. There is no way to lookup a field number and determine 
# if it is an enum or an array in the actual record (since there is 
# the possibility of aliases for FieldDecls). Therefore the FieldDefinition
# must be treated as the source of truth for whether something is an array
# or if it is an enum.

# The FieldType enumeration defines all of the legal field types that can be
# found within a FieldDefinition

class FieldType(IntEnum):
    enum = 0x00
    int8 = 0x01
    uint8 = 0x02
    int16 = 0x83
    uint16 = 0x84
    int32 = 0x85
    uint32 = 0x86
    string = 0x07
    float32 = 0x88
    float64 = 0x89
    uint8z = 0x0a    # non-zero 8 bit unsigned integer
    uint16z = 0x8b
    uint32z = 0x8c
    byte_array = 0x0d

# Record declarations for specific record types. This way, a user can
# simply reference a field like RecordDecl.Speed to retrieve that field
# from a record.

class RecordDecl(IntEnum):
    position_lat = 0
    position_long = 1
    altitude = 2
    heart_rate = 3
    cadence = 4
    distance = 5
    speed = 6
    power = 7
    compressed_speed_distance = 8
    grade = 9
    resistance = 10
    time_from_course = 11
    cycle_length = 12
    temperature = 13
    speed_1s = 17
    cycles = 18
    total_cycles = 19
    compressed_accumulated_power = 28
    accumulated_power = 29
    left_right_balance = 30
    gps_accuracy = 31
    vertical_speed = 32
    calories = 33
    vertical_oscillation = 39
    stance_time_percent = 40
    stance_time = 41
    activity_type = 42
    left_torque_effectiveness = 43
    right_torque_effectiveness = 44
    left_pedal_smoothness = 45
    right_pedal_smoothness = 46
    combined_pedal_smoothness = 47
    time_128 = 48
    stroke_type = 49
    zone = 50
    ball_speed = 51
    cadence_256 = 52
    total_hemoglobin_conc = 54
    total_hemoglobin_conc_min = 55
    total_hemoglobin_conc_max = 56
    saturated_hemoglobin_percent = 57
    saturated_hemoglobin_percent_min = 58
    saturated_hemoglobin_percent_max = 59
    device_index = 62
    time_stamp = 253

# TODO: other types

# Global message declarations - used to lookup up the type of a message

class GlobalMessageDecl(IntEnum):
    file_id = 0
    capabilities = 1
    device_settings = 2
    user_profile = 3
    hrm_profile = 4
    sdm_profile = 5
    bike_profile = 6
    zones_target = 7
    hr_zone = 8
    power_zone = 9
    met_zone = 10
    sport = 12
    goal = 15
    session = 18
    lap = 19
    record = 20
    event = 21
    device_info = 23
    workout = 26
    workout_step = 27
    schedule = 28
    weight_scale = 30
    course = 31
    course_point = 32
    totals = 33
    activity = 34
    software = 35
    file_capabilities = 37
    mesg_capabilities = 38
    field_capabilities = 39
    file_creator = 49
    blood_pressure = 51
    speed_zone = 53
    monitoring = 55
    hrv = 78
    length = 101
    monitoring_info = 103
    pad = 105
    slave_device = 106
    cadence_zone = 131
    memo_glob = 145

# Internal helper methods for reading integers from streams
# TODO: convert these methods into static class methods where we switch on a global
# endian-ness flag

def read_int8(stream):
    return int.from_bytes(stream.read(1), byteorder = "little", signed = True)

def read_uint8(stream):
    return int.from_bytes(stream.read(1), byteorder = "little", signed = False)

def read_int16(stream):
    return int.from_bytes(stream.read(2), byteorder = "little", signed = True)

def read_uint16(stream):
    return int.from_bytes(stream.read(2), byteorder = "little", signed = False)

def read_int32(stream):
    return int.from_bytes(stream.read(4), byteorder = "little", signed = True)

def read_uint32(stream):
    return int.from_bytes(stream.read(4), byteorder = "little", signed = False)

class FileHeader:
    def __init__(self, stream):
        self.stream = stream
        self.size = read_uint8(stream)
        self.protocol_version = read_uint8(stream)
        self.profile_version = read_int16(stream)
        self.data_size = read_uint32(stream)

        # Assert .fit file signature
        b1 = stream.read(1)
        b2 = stream.read(1)
        b3 = stream.read(1)
        b4 = stream.read(1)

        assert b1 == b'.' and b2 == b'F' and b3 == b'I' and b4 == b'T'

        # Read optional CRC
        if self.size > 12:
            self.CRC = int.from_bytes(stream.read(2), byteorder = "little")

# Definitions are runtime objects that are created by parsing the .FIT files.
# Fields and Messages have definitions that must be read from the file being 
# parsed.

class FieldDefinition:
    def __init__(self, stream, current_offset):
        self.field_definition_number = read_uint8(stream)
        self.field_size = read_uint8(stream)
        self.field_offset = current_offset
        self.field_type = read_uint8(stream)

class MessageDefinition:
    def __init__(self, header, stream):
        reserved = read_uint8(stream)

        # 0 == Definition and Data messages are little-endian
        # 1 == Definition and Data messages are big-endian

        # TODO: actually do something with this flag. right now everything
        # that we see is little-endian

        self.architecture = read_uint8(stream)

        self.global_message_number = read_uint16(stream)

        self.field_definitions = []
        field_count = read_uint8(stream)
        current_offset = 0

        for i in range(field_count):
            field_definition = FieldDefinition(stream, current_offset)
            self.field_definitions.append(field_definition)
            current_offset += field_definition.field_size

        self.size = current_offset

    def MessageDefinitionSize(self):
        return len(self.field_definitions) * 3 + 5

# A Message object represents one of the (many) message types 
# that are defined in a .fit file. A Message object contains a 
# byte array that contains the actual data from the file.

class Message:

    # Compute some constants that will be used in this class

    fit_zero_time = datetime(1989, 12, 31, 0, 0, 0, 0)
    posix_zero_time = datetime(1970, 1, 1, 0, 0, 0, 0)
    offset_seconds = fit_zero_time - posix_zero_time

    def __init__(self, header, message_definition, stream):
        self.header = header
        self.message_definition = message_definition
        self.message_data = stream.read(message_definition.size)
        self._is_initialized = False

    # Linear search through a Message's FieldDefinitions 
    # If found, will also guarantee that the internal BinaryReader
    # over the Message is initialized, and pointing at the start
    # of the field.
    # Returns None if not found.

    def _get_field_definition(self, field_number):
        for field_definition in self.message_definition.field_definitions:
            if field_definition.field_definition_number == field_number:
                if not self._is_initialized:
                    self._stream = io.BytesIO(self.message_data)
                    self._is_initialized = True
                self._stream.seek(field_definition.field_offset)
                return field_definition
        return None
                    
    # Retrieve a field from the message, given a field_decl enum whose value
    # is an integer field number.

    def get(self, field_decl):

        field_definition = self._get_field_definition(field_decl)
        if field_definition is not None: 
            field_type = FieldType(field_definition.field_type)

            if field_type is FieldType.int8:
                return read_int8(self._stream)

            elif field_type is FieldType.uint8:
                return read_uint8(self._stream)

            elif field_type is FieldType.uint8z:
                value = read_uint8(self._stream)
                return value if value != 0 else None

            elif field_type is FieldType.int16:
                return read_int16(self._stream)

            elif field_type is FieldType.uint16:
                return read_uint16(self._stream)

            elif field_type is FieldType.uint16z:
                value = read_uint16(self._stream)
                return value if value != 0 else None 

            elif field_type is FieldType.int32:
                return read_int32(self._stream)

            elif field_type is FieldType.uint32:
                return read_uint32(self._stream)

            elif field_type is FieldType.uint32z:
                value = read_uint32(self._stream)
                return value if value != 0 else None

        return None

    # Read the field described by field_decl as a Python datetime object

    def get_as_datetime(self, field_decl):
        field_definition = self._get_field_definition(field_decl)
        if field_definition is not None: 
            field_type = FieldType(field_definition.field_type)

        if field_type is FieldType.uint32:
            timestamp = read_uint32(self._stream)
            if timestamp < 0x10000000:

                # The documentation claims that this is a "system time" value. I don't know what
                # this is - what system? The local system? Right now I'm returning None here 
                # since I don't understand how to compute this value given the information 
                # available. It's entirely possible that this is just a datetime.fromtimestamp()
                # call without the FIT offset computation.

                return None
            else:

                # Need to add difference between UTC 00:00 Dec 31 1989 and UTC 00:00 Jan 01 1970
                # to generate a legal Unix timestamp. This was pre-computed in the class

                return datetime.fromtimestamp(timestamp + Message.offset_seconds.total_seconds())
        else:
            return None

def parse_fit_file(path, validate_crc = False):
    """Parse a fit file.

    Parameters
    ----------
    path: string
        Path to the .fit file
    validate_crc: bool
        Compute the CRC16 of the file and compare with embedded CRC16. Default is False.

    Yields
    ------
    Message
        Individual Message objects from the .fit file.

    Examples
    --------
    >>> for message in parse_fit_file('fit_file.fit'):
            pass
    """

    stream = io.open(path, "rb")
    file_header = FileHeader(stream)

    if validate_crc:
        pos = stream.seek(0)
        crc = compute_crc(stream, file_header.size + file_header.data_size)
        file_crc = read_uint16(stream)

        # TODO: debug why CRC is failing with real files
        # print(pos, format(self.crc, "0x"), format(self.file_crc, "0x"))

        # Seek to the start of the data (past the file header)
        pos = stream.seek(file_header.size)

    bytes_to_read = file_header.data_size
    bytes_read = 0

    # Message definitions are parsed internally by the parser and not exposed to
    # the caller. We store all of them in this dict:
    local_message_definitions = {}

    while bytes_read < bytes_to_read:
        header = read_uint8(stream)

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

            bytes_read += current_message_definition.size + 1