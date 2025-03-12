""" Provide parsing of log content and analytics/reporting from that content """
from csv import reader


def process_log(r: reader):
    for row in r:
        print(', '.join(row))



if __name__ == "__main__":
    with open('logs.log') as log_csv_file:
        r = reader(log_csv_file, delimiter=',', quotechar='"')
        process_log(r)
