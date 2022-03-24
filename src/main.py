# coding=utf-8
###############################################################################
# XRPL Importer
# netcat[dot]av[at]gmail[dot]com
# !/usr/bin/python
###############################################################################
from __future__ import print_function, absolute_import
import sys
import argparse
import logging
import datetime

from source import Source
from importer import Importer
from database import SupportedDatabases



class bcolors:
    HEADER = '\033[94m'
    OKGREEN = '\033[92m'
    WARNING = '\033[93m'
    FAIL = '\033[91m'
    ENDC = '\033[0m'
    BOLD = '\033[1m'
    UNDERLINE = '\033[4m'

if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="XRP Ledger Importer",
        epilog="./importer -s /data -d arangodb -c"
    )

    # main argument
    parser.add_argument('-d', '--database', action="store", required=False,
                        default='arangodb', type=str,
                        help='Database to store the data')
    parser.add_argument('-s', '--source', action="store", required=False,
                        type=str,
                        default='wss://xrplcluster.com',
                        help='Node url or sqlite database direcotry path')
    parser.add_argument('-l', '--ledger', action="store", required=False,
                        default=None, type=int,
                        help='Ledger index start from')
    # optional arguments
    parser.add_argument('-c', '--clean', action='store_const', help='Clean database before import', const=True, default=False)

    args = parser.parse_args()

    # assign args
    SOURCE = args.source
    DATABASE = args.database
    CLEAN = args.clean
    LEDGER_INDEX=args.ledger

    if LEDGER_INDEX and LEDGER_INDEX < 32570:
        print("[!] Ledger index should start from 32570, exiting...")
        exit(1)

    # get database class
    database = next((db for db in SupportedDatabases if db.__name__.lower() == DATABASE), None)

    if not database:
        print("[!] Unable to find selected database! exiting...")
        exit(1)

    START_TIME = datetime.datetime.now().replace(microsecond=0)

    print(bcolors.OKGREEN + "[*] Started at  " + str(START_TIME) + bcolors.ENDC)
    # create database instance
    print(bcolors.OKGREEN + "[!] Connecting to Databse... " + bcolors.ENDC)
    db_instance = database(fresh=CLEAN)

    print(bcolors.OKGREEN + "[!] Connecting to the source [" + SOURCE +"]... " + bcolors.ENDC)
    source = Source(source=SOURCE)

    # start importer
    importer = Importer(source=source, database=db_instance, ledger=LEDGER_INDEX)

    try:
        importer.start()
    except KeyboardInterrupt as e:
        print("[!] Caught keyboard interrupt. Canceling tasks...")
    finally:
        importer.stop()


    END_TIME = datetime.datetime.now().replace(microsecond=0)

    print(bcolors.OKGREEN + "[*] Ended at  " + str(END_TIME) + bcolors.ENDC)






