#! /usr/bin/env python3

import sys


def run():
    for line in sys.stdin:
        sys.stdout.write(line)
    return 0

if __name__ == "__main__":
    exit(run())
