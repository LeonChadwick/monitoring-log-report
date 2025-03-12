""" Provide parsing of log content and analytics/reporting from that content """
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
    timestamp: str
    description: str
    status: str
    pid: str

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


class LogAnalyser:
    """ Tracking start/stop for jobs via dict, using (description, pid) as the key.
        TODO: Am making assumption at this time that concurrent runs with same description but different pid is ok, but this needs to be confirmed.
     """

    def __init__(self):
        self._runs = {}

    def process_line(self, l: LogLine):
        if l.status == JobStatus.START:
            # Record start of a job
            self._runs[l.get_key()] = l
        elif l.status == JobStatus.END:
            # Analyse now job end has been provided
            prior = self._runs[l.get_key()]
            if prior is None:
                # Edge case where log hasn't captured the start time of a job - ignoring
                pass
            else:
                elapsed = l.timestamp - prior.timestamp
                print(f"elapsed={elapsed}")
        else:
            raise ValueError("Unhandled job status")


if __name__ == "__main__":
    analyser = LogAnalyser()

    # we don't read all the content into memory at once which could cause out of memory and if the analysis were
    # to be run on a production machine could impact memory requirements of production services, so we process as we read each line.
    with open('logs.log') as log_csv_file:
        # skipinitialspace is used here as status column data seems to have whitespace
        r = DictReader(log_csv_file, fieldnames=LOG_COLUMN_NAMES, delimiter=',', quotechar='"', skipinitialspace=True)
        for line in r:
            ll = LogLine.build(line)
            analyser.process_line(ll)

