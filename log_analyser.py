""" Provide parsing of log content and analytics/reporting from that content """
import logging
import sys
from time import strftime

from log_parser import LogLine, JobStatus, parse_log, open_input, LOG_TIMESTAMP_FORMAT

logger = logging.getLogger(__name__)

class LogAnalyser:
    """ Tracking start/stop for jobs via dict, using (description, pid) as the key.
        TODO: Am making assumption at this time that concurrent runs with same description but different pid is ok, but this needs to be confirmed.
     """


    def __init__(self):
        self.WARN_THRESHOLD = 5 * 60
        self.ERROR_THRESHOLD = 10 * 60

        self._runs = {}

    def process_line(self, l: LogLine):
        assert(self.ERROR_THRESHOLD >= self.WARN_THRESHOLD)  # the checks below rely on this ordering, would require more coding to support that variant

        key = l.get_key()
        if l.status == JobStatus.START:
            # Record start of a job
            if key in self._runs:
                # Expectations for this edge case behaviour are undefined, have coded to log at debug level
                # but am tracking elapsed time from the earlier start and not the concurrent start to prevent
                # hiding a problem if the jobs do overrun
                logger.debug("un-terminated concurrent run logged for job=%s pid=%d start_time=%s",\
                            l.description, l.pid, l.timestamp.strftime(LOG_TIMESTAMP_FORMAT))
            else:
                self._runs[key] = l
        elif l.status == JobStatus.END:
            # Analyse now job end has been provided
            prior = self._runs.get(key, None)
            if prior is None:
                # Our log cut/rollover may have missed out the start of a job but our log captured the end
                # we don't have sufficient information to track elapsed time
                # so just logging this edge case and moving on
                # TODO we could consider taking the start time of the very first entry in the log as a guidance
                #     on elapsed time
                logger.debug("un-recorded start for job=%s pid=%d end_time=%s",\
                            l.description, l.pid, l.timestamp.strftime(LOG_TIMESTAMP_FORMAT))
                return

            del self._runs[key]
            if prior is None:
                # Edge case where log hasn't captured the start time of a job - ignoring
                pass
            else:
                elapsed_seconds = (l.timestamp - prior.timestamp).total_seconds()
                if elapsed_seconds < self.WARN_THRESHOLD:
                    return
                # report problem has been detected based upon thresholds defined
                level = logging.ERROR if elapsed_seconds >= self.ERROR_THRESHOLD else logging.WARNING
                logger.log(level, "%ds elapsed for job=%s pid=%d end_time=%s",\
                            elapsed_seconds, l.description, l.pid, l.timestamp.strftime(LOG_TIMESTAMP_FORMAT))
        else:
            raise ValueError("Unhandled job status")


if __name__ == "__main__":
    logging.basicConfig(format='%(levelname)7s:%(message)s', level=logging.INFO)
    analyser = LogAnalyser()

    log_csv_input = None
    try:
        log_csv_input = open_input()
        for ll in parse_log(log_csv_input):
            analyser.process_line(ll)
    finally:
        if log_csv_input and log_csv_input is not sys.stdin:
            log_csv_input.close()
