#!/usr/bin/env python

from datetime import datetime
from time import time

#debug=0 means no verbose mode, debug!=0 means verbose mode
#setting debug to 0 significantly reduces the size of the log
#file as all the spew logs are no more dumped into the log file.
debug=0

class LogCollector(object):
    def __init__(self, logfile):
        """
        write logs in the file passed to the constructor of the class.
        """
        self.logfile = logfile
        
    def closelog(self):
        self.logfile.close()

    def now(self, sep=None):
        return datetime.now().isoformat(sep)

    def log(self,verbose=0, compact=0):
        if verbose and debug:
            self.logfile.write(self.now(' ') + ' ' + str(time()) + ' DEBUG ' + verbose + '\n')
        if compact:
            #self.logfile.write(self.now(' ') + ' ' + compact + '\n')
            #self.logfile.write(str(time()) + ' ' + compact + '\n')            
            self.logfile.write(("%.6f" % time()) + ' ' + compact + '\n')            



if __name__ == '__main__':
    """
    Test of the class LogCollector.
    """
    import sys
    collector=LogCollector(sys.argv[1])
    print collector.now(' ')
    for i in sys.argv[2:]:
        collector.log(i)
    collector.closelog()
