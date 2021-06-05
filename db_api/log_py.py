from datetime import datetime  # importing DateTime package


class App_Logger:
    '''
    It is used save logs into a file

    Parameters
    ----------
    file: log file name Default is logfile.log

    '''

    def __init__(self, file="logfile.log"):
        self.f_name = file

    def log(self, log_type, log_msg):
        '''
        Function log to save logs and log type in file

        Parameters
        ----------
        log_type: Type of log-info,error,warning etc
        log_msg: Log to be saved(message)

        '''
        now = datetime.now()  # current time
        current_time = now.strftime("%d-%m-%Y %H:%M:%S")  # changing time format
        f = open(self.f_name, "a+")  # opening file in append + mode
        f.write(current_time + "," + log_type + "," + log_msg + "\n")  # writing log type and message in log file
        f.close()  # closing log file
