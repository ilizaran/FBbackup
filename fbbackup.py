# Copyright (c) 2020 Ignacio José Lizarán Rus <ilizaran@gmail.com>
#
# Permission to use, copy, modify, and distribute this software for any
# purpose with or without fee is hereby granted, provided that the above
# copyright notice and this permission notice appear in all copies.
#
# THE SOFTWARE IS PROVIDED "AS IS" AND THE AUTHOR DISCLAIMS ALL WARRANTIES
# WITH REGARD TO THIS SOFTWARE INCLUDING ALL IMPLIED WARRANTIES OF
# MERCHANTABILITY AND FITNESS. IN NO EVENT SHALL THE AUTHOR BE LIABLE FOR
# ANY SPECIAL, DIRECT, INDIRECT, OR CONSEQUENTIAL DAMAGES OR ANY DAMAGES
# WHATSOEVER RESULTING FROM LOSS OF USE, DATA OR PROFITS, WHETHER IN AN
# ACTION OF CONTRACT, NEGLIGENCE OR OTHER TORTIOUS ACTION, ARISING OUT OF
# OR IN CONNECTION WITH THE USE OR PERFORMANCE OF THIS SOFTWARE.

import argparse
import getpass
import os
import time

from fbcore import FbBackup


def main():
    tic = time.clock()
    parser = argparse.ArgumentParser(description="Firebird Backup")
    parser.add_argument('-u', '--user', 
                        dest='user',
                        type=str, 
                        required=True, 
                        help="user name")
    parser.add_argument('-p', '--password', 
                        dest='password', 
                        metavar="password",
                        nargs='?', 
                        required=True, 
                        help="connection password")
    parser.add_argument('-d', '--dsn', 
                        dest='dsn', 
                        type=str,
                        help="dsn database", 
                        required=True)
    parser.add_argument('-s', '--sql_dialect', 
                        dest='sql_dialect',
                        type=int, 
                        help="SQL dialect", 
                        default=3)
    parser.add_argument('-c', '--charset', 
                        dest='charset',
                        type=str, 
                        help="connection charset", 
                        default='UTF8')
    parser.add_argument(dest='table', 
                        type=str,
                        help="table name to backup or all")

    parsed_args = vars(parser.parse_args())

    try:
        if parsed_args['password'] is None:
            parsed_args['password'] = getpass.getpass()

        fbba = FbBackup(parsed_args)

        if parsed_args['table'] != 'all':
            fbba.set_table(parsed_args['table'])
        fbba.backup_tables()

    except Exception as err:
        print('ERROR:', err)
        os._exit(1)

    toc = time.clock()
    print('Elapse time: %s' % (toc - tic))


if __name__ == "__main__":
    main()
