#!/usr/bin/env python3
""" module"""
import shutil

from parsl.app.watcher import bash_watch
from parsl.data_provider.dynamic_files import DynamicFileList

import os

import time
import parsl
from parsl.monitoring.monitoring import MonitoringHub
from parsl.config import Config
from parsl.executors import ThreadPoolExecutor, HighThroughputExecutor
from parsl.addresses import address_by_hostname
from parsl.data_provider.files import File

parsl.clear()
config = Config(
    executors=[
        HighThroughputExecutor(
            label="local_htex"
            ),
        ThreadPoolExecutor(
            label='bash_watch_exec',
            max_threads=2,
            thread_name_prefix='bash_watch'
            )
        ],
    monitoring=MonitoringHub(
        hub_address=address_by_hostname(),
        hub_port=55055,
        monitoring_debug=True,
        resource_monitoring_enabled=True,
        resource_monitoring_interval=1,
        file_provenance=True),
    strategy='none'
    )

parsl.load(config)


@parsl.bash_app(executors=['local_htex'])
def initialize(outputs=None):
    """Just create a small file via bash"""
    import time
    import os
    time.sleep(1)
    return f"echo 'Initialized' > {os.path.join(os.getcwd(), 'initialize.txt')}"


@parsl.python_app(executors=['local_htex'])
def split_data(inputs=None, outputs=None):
    """Read the input file and write it to 4 output files"""
    with open(inputs[0], 'r') as fh:
        data = fh.read()
    for i, op in enumerate(outputs):
        with open(op, 'w') as ofh:
            ofh.write(f"{i + 1}\n")
            ofh.write(data)


@parsl.python_app(executors=['local_htex'])
def process(inputs=None, outputs=None):
    """Do something with the input files"""
    import time
    with open(inputs[0], 'r') as fh:
        data = fh.read()
    time.sleep(int(data[0]))
    with open(outputs[0], 'w') as ofh:
        ofh.write(f"{data} processed")


@parsl.python_app(executors=['local_htex'])
def combine(inputs=None, outputs=None):
    """ comb """
    with open(outputs[0], 'w') as ofh:
        for fl in inputs:
            with open(fl, 'r') as fh:
                ofh.write(fh.read())
                ofh.write("\n")


if __name__ == '__main__':
    cwd = os.getcwd()
    if os.path.exists('provenance'):
        shutil.rmtree('provenance')

    os.mkdir(os.path.join(os.getcwd(), 'provenance'))
    os.chdir(os.path.join(os.getcwd(), 'provenance'))

    inp = DynamicFileList()

    init = bash_watch('bash_watch_exec', initialize, inp, [os.getcwd()])
    init.result()

    sd = split_data(inputs=[init.outputs[0]],
                    outputs=[File(os.path.join(os.getcwd(), f'split_data_{i}.txt')) for i in range(4)])

    p = [process(inputs=[sdo], outputs=[File(os.path.join(os.getcwd(), f'processed_data_{i}.txt'))]) for i, sdo in
         enumerate(sd.outputs)]

    c = combine(inputs=[pp.outputs[0] for pp in p], outputs=[File(os.path.join(os.getcwd(), 'combined_data.txt'))])

    c.result()
    os.chdir(cwd)
    time.sleep(2)
