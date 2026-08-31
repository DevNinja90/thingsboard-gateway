#     Copyright 2026. ThingsBoard
#
#     Licensed under the Apache License, Version 2.0 (the "License");
#     you may not use this file except in compliance with the License.
#     You may obtain a copy of the License at
#
#         http://www.apache.org/licenses/LICENSE-2.0
#
#     Unless required by applicable law or agreed to in writing, software
#     distributed under the License is distributed on an "AS IS" BASIS,
#     WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#     See the License for the specific language governing permissions and
#     limitations under the License.

import struct
import snap7.util
from snap7.tags import Tag

from datetime import datetime, date, timedelta

from thingsboard_gateway.tb_utility.tb_utility import TBUtility


class S7DownlinkConverter:
    def __init__(self, logger):
        self._log = logger

    def convert(self, config, data):
        request_type = config.get('type')
        if request_type == 'data':
            return self._convert_data_request(config, data)
        elif request_type == 'tag':
            return self._convert_tag_request(config, data)
        elif request_type == 'vm':
            return self._convert_vm_request(data)
        else:
            self._log.error(
                f"Unsupported request type '{request_type}' for downlink conversion.")
            return None

    def _convert_data_request(self, config, data):
        data_type = str(config.get("dataType", "raw")).lower().strip()
        offset = config.get("offset", 0)
        bit_index = config.get("bit", config.get("bitIndex", 0))
        specified_size = config.get("size", 0)

        type_sizes = {
            "bool": 1,
            "boolean": 1,
            "bit": 1,
            "byte": 1,
            "usint": 1,
            "uint8": 1,
            "sint": 1,
            "int8": 1,
            "int": 2,
            "int16": 2,
            "short": 2,
            "uint": 2,
            "uint16": 2,
            "word": 2,
            "dint": 4,
            "int32": 4,
            "udint": 4,
            "uint32": 4,
            "dword": 4,
            "real": 4,
            "float": 4,
            "float32": 4,
            "lreal": 8,
            "double": 8,
            "float64": 8,
        }

        min_size = type_sizes.get(data_type, 0)

        if data_type in ("string", "str", "s7string"):
            max_len = config.get(
                "maxLength", specified_size - 2 if specified_size > 2 else 254)
            min_size = max_len + 2
        elif data_type in ("raw", "bytes", "bytearray", "array"):
            min_size = len(data)

        buf_size = max(specified_size, offset + min_size)
        buf = bytearray(buf_size)

        if data_type in ("bool", "boolean", "bit"):
            bool_value = TBUtility.str_to_bool(data)
            snap7.util.set_bool(buf, offset, bit_index, bool_value)

        elif data_type in ("byte", "usint", "uint8"):
            snap7.util.set_usint(buf, offset, int(data))

        elif data_type in ("sint", "int8"):
            struct.pack_into(">b", buf, offset, int(data))

        elif data_type in ("int", "int16", "short"):
            snap7.util.set_int(buf, offset, int(data))

        elif data_type in ("uint", "uint16", "word"):
            snap7.util.set_uint(buf, offset, int(data))

        elif data_type in ("dint", "int32"):
            snap7.util.set_dint(buf, offset, int(data))

        elif data_type in ("udint", "uint32", "dword"):
            snap7.util.set_dword(buf, offset, int(data))

        elif data_type in ("real", "float", "float32"):
            snap7.util.set_real(buf, offset, float(data))

        elif data_type in ("lreal", "double", "float64"):
            struct.pack_into(">d", buf, offset, float(data))

        elif data_type in ("string", "str", "s7string"):
            str_val = str(data)
            max_len = config.get(
                "maxLength", specified_size - 2 if specified_size > 2 else 254)
            snap7.util.set_string(buf, offset, str_val, max_len)

        elif data_type in ("raw", "bytes", "bytearray", "array"):
            raw_bytes = bytearray(data)
            buf[offset: offset + len(raw_bytes)] = raw_bytes

        else:
            raise ValueError(f"Unsupported dataType: '{data_type}'")

        return buf

    def _convert_tag_request(self, config, data):
        tag_str = config.get('tag')

        try:
            tag = Tag.from_string(tag_str)
        except Exception as e:
            self._log.error(f"Failed to parse tag '{tag_str}': {e}")
            return None

        if tag.is_symbolic:
            self._log.error(
                f"Failed to process tag '{tag_str}': Symbolic (LID-based) tag access is not supported")
            return None

        datatype = tag.datatype.upper()

        try:
            if tag.count > 1:
                if not isinstance(data, (list, tuple)):
                    self._log.error(
                        f"Failed to convert value for tag '{tag_str}': "
                        f"Expected list/tuple for array (count={tag.count}), got {data!r}")
                    return None
                return [self._convert_scalar(datatype, v, tag_str) for v in data]

            return self._convert_scalar(datatype, data, tag_str)
        except (ValueError, TypeError) as e:
            self._log.error(
                f"Failed to convert value '{data}' to type '{datatype}' for tag '{tag_str}': {e}")
            return None

    def _convert_vm_request(self, data):
        try:
            return int(data)
        except (ValueError, TypeError) as e:
            self._log.error(
                f"Failed to convert value '{data}' to int for vm downlink conversion: {e}"
            )
            return None

    def _convert_scalar(self, datatype, value, tag_str):

        if datatype == 'BOOL':
            return TBUtility.str_to_bool(value)

        elif datatype in ('BYTE', 'SINT', 'USINT', 'INT', 'UINT', 'WORD',
                          'DINT', 'UDINT', 'DWORD', 'LINT', 'ULINT', 'LWORD'):
            return int(value)

        elif datatype in ('REAL', 'LREAL'):
            return float(value)

        elif datatype in ('CHAR', 'WCHAR'):
            return str(value)[:1]

        elif datatype.startswith(('STRING', 'FSTRING', 'WSTRING')):
            return str(value)

        elif datatype == 'TIME':
            # TODO: Add parser to convert milliseconds (int) or ISO durations into S7 duration format ('T#...').
            return str(value)

        elif datatype == 'TOD':
            # TODO: Convert string 'HH:MM:SS' or milliseconds (int) into valid S7 TOD format.
            t = datetime.strptime(str(value), "%H:%M:%S.%f" if '.' in str(value) else "%H:%M:%S").time()
            return timedelta(hours=t.hour, minutes=t.minute, seconds=t.second, microseconds=t.microsecond)

        elif datatype == 'DATE':
            # TODO: Convert ISO date string ('YYYY-MM-DD') into S7 DATE format.
            return date.fromisoformat(str(value))

        elif datatype in ('DT', 'DTL'):
            # TODO: Convert ISO datetime string into S7 BCD/DTL structure.
            return datetime.fromisoformat(str(value))

        elif datatype in ('LTIME', 'LTOD', 'LDT'):
            # TODO: Implement 64-bit S7 time types (LTIME, LTOD, LDT) conversion.
            return value

        else:
            self._log.warning(
                f"Unsupported datatype '{datatype}' for tag '{tag_str}', passing value as-is")
            return value
