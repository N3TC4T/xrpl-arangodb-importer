# coding=utf-8
###############################################################################
# XRPL Arangodb Importer
# netcat[dot]av[at]gmail[dot]com
# !/usr/bin/python
###############################################################################
from __future__ import print_function, absolute_import
import sys
import argparse
import logging
import datetime

from lib import Database, Connection, Importer

try:
    from pyArango.theExceptions import ValidationError
except ImportError:
    print("pyArango package is required!")
    sys.exit(1)

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
        description="XRP Ledger Arangodb Importer",
        epilog="./importer -h http://127.0.0.1:829 -u arangodb_username -p arangodb_pass -d -v"
    )

    # required argument
    parser.add_argument('-ah', '--host', action="store", required=False,
                        default='http://127.0.0.1:8529', type=str,
                        help='Arangodb url')
    parser.add_argument('-u', '--username', action="store", required=False,
                        default='root',type=str,
                        help='Arangodb username')
    parser.add_argument('-p', '--password', action="store", required=False,
                       default='', type=str,
                        help='Arangodb password')
    parser.add_argument('-s', '--server', action="store", required=False,
                        type=str,
                        default='wss://rippled.xrptipbot.com:443',
                        help='Rippled server')
    parser.add_argument('-l', '--ledger', action="store", required=False,
                        default=32570, type=int,
                        help='Ledger index start from')
    # optional arguments
    parser.add_argument('-c', '--clean', action='store_const', help='Clean database before import', const=True, default=False)
    parser.add_argument('-d', '--debug', action='store_const', help='Debug mode', const=True, default=False)

    args = parser.parse_args()

    # assign args
    HOST = args.host
    USERNAME = args.username
    PASSWORD = args.password
    SERVER = args.server
    CLEAN = args.clean
    DEBUG = args.debug
    LEDGER_INDEX=args.ledger

    if LEDGER_INDEX < 32570:
        print("Ledger index should start from 32570")
        exit(1)

    # Logging stuff
    logging.basicConfig(stream=sys.stdout, format="[%(filename)s:%(lineno)s - %(funcName)10s() : %(message)s")
    logger = logging.getLogger(__name__)
    logger.setLevel(level=loggin.DEBUG if DEBUG else logging.DEBUG)


    START_TIME = datetime.datetime.now()


    print(bcolors.OKGREEN + "[*] Started at  " + str(START_TIME) + bcolors.ENDC)
    # create database instance
    print(bcolors.OKGREEN + "[!] Connecting to Databse ... " + bcolors.ENDC)
    db = Database(host=HOST, username=USERNAME, password=PASSWORD, fresh=CLEAN)


    print(bcolors.OKGREEN + "[!] Connecting to Server ... " + bcolors.ENDC)
    conn = Connection(server=SERVER)


    # start importer
    imp = Importer(connection=conn, database=db, logger=logger, startLedger=LEDGER_INDEX)

    # start the importing
    imp.start()



