# -*- coding: utf-8 -*-
"""
@author: Irache Garamendi
"""

import logging
import sys

from pyflink.common import WatermarkStrategy, Encoder, Types
from pyflink.datastream import StreamExecutionEnvironment, RuntimeExecutionMode
from pyflink.datastream.connectors import (FileSource, StreamFormat, FileSink, OutputFileConfig,
                                           RollingPolicy)
from pyflink.common import Types, Row
from pyflink.table import *
from pyflink.table.expressions import col


def show(ds, env):
    ds.print()
    env.execute()


def count_on_device(ds, env, area):
    # Row(dateTime=w[0], idDevice=w[1], propertyDevice=w[2], valuePropertyDevice=w[3], area=w[4]
    dsfilterdevicecaracteristics = ds.filter(lambda x: x[2] == 'sensedPresence' and x[3] == 'on')

    dsfilterArea = dsfilterdevicecaracteristics.filter(lambda x: x[4] == area)

    dsfilterBedroom = dsfilterArea.map(lambda x: (x[4] + '-' + x[2] + '-' + x[3], 1),
                                       output_type=Types.TUPLE([Types.STRING(), Types.INT()])) \
        .key_by(lambda x: x[0]) \
        .reduce(lambda x, j: (x[0], x[1] + j[1]))
    dsfilterBedroom.print()
    return dsfilterBedroom


def set_ds(env):
    input_path = "/mnt/c/flink-1.15.2/bin/1.csv"
    ds = env.from_source(
        source=FileSource.for_record_stream_format(StreamFormat.text_line_format(),
                                                   input_path)
            .process_static_file_set().build(),
        watermark_strategy=WatermarkStrategy.for_monotonous_timestamps(),
        source_name="file_source",
        type_info=Types.STRING()
    )

    ds = ds.map(lambda x: x.split('\n')).map(lambda x: x[0]).map(lambda x: x.split(','))

    return ds


def set_env():
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(1)
    return env


if __name__ == '__main__':
    logging.basicConfig(stream=sys.stdout, level=logging.INFO, format="%(message)s")
    env = set_env()
    ds = set_ds(env)

    dscountOnDeviceChair_bedroom = count_on_device(ds, env, 'chair_bedroom')
    show(dscountOnDeviceChair_bedroom, env)

    dscountOnDeviceWardrobe = count_on_device(ds, env, 'wardrobe')
    show(dscountOnDeviceWardrobe, env)

    dscountOnDeviceBedroom = count_on_device(ds, env, 'bedroom')
    show(dscountOnDeviceBedroom, env)

    dscountOnDeviceSleep = count_on_device(ds, env, 'sleep')
    show(dscountOnDeviceSleep, env)
