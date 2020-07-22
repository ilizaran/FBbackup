# MIT License

# Copyright (c) 2020 Ignacio José Lizarán Rus

# Permission is hereby granted, free of charge, to any person obtaining a copy
# of this software and associated documentation files (the "Software"), to deal
# in the Software without restriction, including without limitation the rights
# to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
# copies of the Software, and to permit persons to whom the Software is
# furnished to do so, subject to the following conditions:

# The above copyright notice and this permission notice shall be included in all
# copies or substantial portions of the Software.

# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
# AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
# OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
# SOFTWARE.

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
