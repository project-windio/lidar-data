""" Configuration for the script that simulates lidar data ("sim_lidar_data.py") """


import argparse
import json
import sys
import signal
import logging

LIDAR_TOPIC = "ldr".encode('utf-8')

def signal_handler_exit(sig, frame):
    print('* msb_ldr: bye')
    sys.exit(0)


def dump_config_file(config: dict):
    with open(config['dump_config_file'], 'w') as config_file_handle:
        config_file_handle.writelines(
            json.dumps(
                config,
                indent=4
            )
        )


def read_parse_config_file(config: dict) -> dict:

    try:
        config_file_handler = open(config['config_file'], 'r')
    except Exception as e:
        print(f'failed to open config file: {e}')
        sys.exit(-1)

    config_file_args = json.load(config_file_handler)

    for key, value in config_file_args.items():
        if key == 'config_file':
            continue

        if key in config:

            print(f'parsing {key} : {value}')
            config[key] = value
        else:
            print(f'key not found: {key} omitting')

    return config
    # 1. read config file
    # 2. convert from json to dict
    # 3. iterate over entries in dictionary and override parsed arguments

# build a config named tuple


def parse_arguments() -> dict:
    arg_parser = argparse.ArgumentParser()

    arg_parser.add_argument(
        '--verbose',
        action='store_true',
        help='for debugging purposes'
    )

    arg_parser.add_argument(
        '--print',
        action='store_true',
        help='use this flag to print data to stdout'
    )

    arg_parser.add_argument(
        '--logfile',
        help='path to logfile',
        type=str,
        # default=f'/tmp/msb_gps_{datetime.now().astimezone().strftime("%Y-%m-%dT%H-%M-%S%z")}.log',
        default='',
    )

    arg_parser.add_argument(
        '--config-file',
        help='configuration file: overwrite all commandline options!',
        default='',
        type=str,
    )

    arg_parser.add_argument(
        '--dump-config-file',
        help='dumps the default config values into a file',
        default='',
    )

    arg_parser.add_argument(
        '--ipc-port',
        help='IPC port used by zeroMQ',
        default=5556,
        type=int
    )

    arg_parser.add_argument(
        '--ipc-protocol',
        help='the protocol used for IPC with zeroMQ',
        default='tcp://127.0.0.1',
        type=str,
    )

    arg_parser.add_argument(
        '--modbus-protocol',
        help='the modbus protocol used to extract lidar data',
        default='10.10.8.1',
        type=str,
    )

    arg_parser.add_argument(
        '--modbus-port',
        help='the modbus port used to extract lidar data',
        default='502',
        type=str,
    )

    return arg_parser.parse_args().__dict__


def init() -> dict:

    signal.signal(signal.SIGINT, signal_handler_exit)

    config = parse_arguments()

    logging.basicConfig(
        filename=config['logfile'],
        level=logging.DEBUG if config['verbose'] else logging.WARNING,
        format='%(levelname)s: %(asctime)s %(message)s',
        datefmt='%Y%m%dT%H%M%S%z',
    )

    logging.debug('msb_gps.py parsing of configuration done')

    if config['config_file']:
        logging.debug('parsing config file')
        config = read_parse_config_file(config)
        logging.debug(f'updated config file: {config}')

    if config['dump_config_file']:
        logging.debug(f'dumping config file to {config["dump_config_file"]}')
        dump_config_file(config)

    return config
