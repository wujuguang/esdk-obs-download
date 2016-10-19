#!/usr/bin/env python
# coding: utf-8

"""查看已下载数量.
"""

from __future__ import print_function

import os
import sys
import time

from conf import download_storage_path


def main():
    if len(sys.argv) < 2 or sys.argv[1] not in ['local', 'online']:
        print('参数错误')
        exit(0)

    storage_path = download_storage_path[sys.argv[1]]
    while 1:
        os.system('du %s -sh' % storage_path)
        time.sleep(60)


if __name__ == '__main__':
    main()
