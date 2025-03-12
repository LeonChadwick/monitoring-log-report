# monitoring-log-report
An assignment for parsing and reporting based upon logged data.


# Open Questions To Enquire On With Stakeholder(s):
- is the whitespace on the START/STOP status column data guaranteed to be there or should whitespace stripping be implemented?
- would a job with same description running concurrently but with different pid be a valid scenario or should flag as error if detected?


# Description

This application parses its input data collecting statistics on time taken for jobs
that have logged their run start and end times and reporting those jobs
that exceed 5 minutes (as a warning) or exceed 10 minutes (as an error).

Log input is expected in CSV format and is headerless, i.e. the first row is data just like all the other rows.

Log columns are:
- HH:MM:SS timestamp
- Job description
- enum of {START|END}
- process id (PID)


# IMPORTANT

This app expects log file content to either be piped/redirect into it or the log file to be supplied as sole command
line argument.
Running in IDE must be performed by passing the filename or will hang expecting an input pipe that you haven't supplied.