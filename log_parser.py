""" Provide parsing of log content and analytics/reporting from that content """
import sys
from csv import DictReader
from dataclasses import dataclass
from datetime import datetime
from enum import Enum


# Definitions that describe the structure and content of the log file
class JobStatus(Enum):
    START = "START"
    END = "END"

LOG_COLUMN_NAMES = ("timestamp", "description", "status", "pid")
LOG_TIMESTAMP_FORMAT = "%H:%M:%S"

@dataclass
class LogLine:
    timestamp: datetime.time
    description: str
    status: JobStatus
    pid: int

    def get_key(self):
        """ Identity of a job run in a log line """
        return (self.description, self.pid)

    @staticmethod
    def build(d: dict):
        d["timestamp"] = datetime.strptime(d["timestamp"], LOG_TIMESTAMP_FORMAT)
        d["status"] = JobStatus[d["status"]]
        d["pid"] = int(d["pid"])
        ll = LogLine(**d)
        return ll


def parse_log(l):
    """ parse lines of log file and yield a correctly typed LogLine object for each input line """
    # we don't read all the content into memory at once which could cause out of memory and if the analysis were
    # to be run on a production machine could impact memory requirements of production services, so we process as we read each line.
    # skipinitialspace is used here as status column data seems to have whitespace
    r = DictReader(l, fieldnames=LOG_COLUMN_NAMES, delimiter=',', quotechar='"', skipinitialspace=True)
    for line in r:
        ll = LogLine.build(line)
        yield ll


def open_input():
    """ return handle to input file based upon whether user has piped/redirected input or has specified filename on command line
    """
    log_input = None
    if len(sys.argv) == 2:
        file_name = sys.argv[1]
        log_input = open(file_name, 'r')
    else:
        # expecting piped input.  If running/debugging inside an IDE, ensure you use the filename option above instead
        log_input = sys.stdin

    return log_input

if __name__ == "__main__":
    # just reads and parses, don't do anything with the result (no output expected)
    log_csv_input = None
    try:
        log_csv_input = open_input()
        parse_log(log_csv_input)
    finally:
        if log_csv_input and log_csv_input is not sys.stdin:
            log_csv_input.close()
