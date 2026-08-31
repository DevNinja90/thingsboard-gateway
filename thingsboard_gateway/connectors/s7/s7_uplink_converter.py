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

from time import time
import struct

import snap7

from thingsboard_gateway.connectors.s7.s7_converter import S7Converter
from thingsboard_gateway.gateway.statistics.statistics_service import StatisticsService
from thingsboard_gateway.gateway.entities.converted_data import ConvertedData
from thingsboard_gateway.gateway.entities.report_strategy_config import ReportStrategyConfig
from thingsboard_gateway.tb_utility.tb_utility import TBUtility


class S7UplinkConverter(S7Converter):
    def __init__(self, logger, config):
        self.__log = logger
        self.__config = config

    def convert(self, data):
        StatisticsService.count_connector_message(
            self.__log.name, 'convertersMsgProcessed')
        config = {
            'attributes': self.__config.attributes,
            'timeseries': self.__config.timeseries
        }

        converted_data = ConvertedData(
            device_name=self.__config.device_name, device_type=self.__config.device_profile_name)
        converted_data_append_methods = {
            'attributes': converted_data.add_to_attributes,
            'timeseries': converted_data.add_to_telemetry
        }

        device_report_strategy = self._get_device_report_strategy(self.__config.report_strategy_config,
                                                                  self.__config.device_name)

        received_data_ts = int(time() * 1000)

        for config, value in zip(self.__config.datapoints, data):
            try:
                datapoint_key = TBUtility.convert_key_to_datapoint_key(config['key'],
                                                                       device_report_strategy,
                                                                       config,
                                                                       self.__log)
                converted_value = self.convert_data(config, value)
                payload = {datapoint_key: converted_value}
                if config['type_'] == 'timeseries':
                    payload['ts'] = received_data_ts

                converted_data_append_methods[config['type_']](payload)
            except Exception as e:
                self.__log.exception(
                    "Failed to convert data for device '%s' datapoint '%s': %s", self.__config.device_name, config['key'], e)  # noqa: E501
                StatisticsService.count_connector_message(
                    self.__log.name, 'convertersError', count=1)
                continue

        StatisticsService.count_connector_message(self.__log.name,
                                                  'convertersAttrProduced',
                                                  count=converted_data.attributes_datapoints_count)
        StatisticsService.count_connector_message(self.__log.name,
                                                  'convertersTsProduced',
                                                  count=converted_data.telemetry_datapoints_count)

        self.__log.debug("Converted data: %s", converted_data)
        return converted_data

    def _get_device_report_strategy(self, report_strategy, device_name):
        try:
            return ReportStrategyConfig(report_strategy)
        except ValueError as e:
            self.__log.trace(
                "Report strategy config is not specified for device %s: %s", device_name, e)

    def convert_data(self, config, value):
        if config['type'] == 'vm' or config['type'] == 'tag':
            return value
        elif config['type'] == 'data':
            return self._convert_data_type(config, value)
        else:
            raise ValueError(
                f"Unsupported datapoint type '{config['type']}' for device '{self.__config.device_name}'")

    def _convert_data_type(self, config, value):
        if not value:
            return None

        # Normalize data type key and handle optional byte offset within buffer
        data_type = str(config.get("dataType", "raw")).lower().strip()
        offset = config.get("offset", 0)
        bit_index = config.get("bit", config.get("bitIndex", 0))

        if data_type in ("bool", "boolean", "bit"):
            return snap7.util.get_bool(value, offset, bit_index)

        elif data_type in ("byte", "usint", "uint8"):
            return snap7.util.get_usint(value, offset)

        elif data_type in ("sint", "int8"):
            return int.from_bytes(value[offset:offset + 1], byteorder="big", signed=True)

        elif data_type in ("int", "int16", "short"):
            return snap7.util.get_int(value, offset)

        elif data_type in ("uint", "uint16", "word"):
            return snap7.util.get_uint(value, offset)

        elif data_type in ("dint", "int32"):
            return snap7.util.get_dint(value, offset)

        elif data_type in ("udint", "uint32", "dword"):
            return snap7.util.get_dword(value, offset)

        elif data_type in ("real", "float", "float32"):
            return float(snap7.util.get_real(value, offset))

        elif data_type in ("lreal", "double", "float64"):
            return float(struct.unpack_from(">d", value, offset)[0])

        elif data_type in ("string", "str", "s7string"):
            return snap7.util.get_string(value, offset)

        elif data_type in ("raw", "bytes", "bytearray", "array"):
            # bytearray is not JSON-serializable; convert to list of ints
            return list(value[offset:])

        else:
            raise ValueError(f"Unsupported dataType: '{data_type}'")
