# monitoring-log-report
An assignment for parsing and reporting based upon logged data.


This application parses its input data collecting statistics on time taken for jobs
that have logged their run start and end times and reporting those jobs
that exceed 5 minutes (as a warning) or exceed 10 minutes (as an error).

Log input is expected in CSV format.

Log columns are:
- HH:MM:SS timestamp
- Job description
- enum of {START|END}
- process id (PID)



