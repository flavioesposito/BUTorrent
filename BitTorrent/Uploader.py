# The contents of this file are subject to the BitTorrent Open Source License
# Version 1.0 (the License).  You may not copy or use this file, in either
# source code or executable form, except in compliance with the License.  You
# may obtain a copy of the License at http://www.bittorrent.com/license/.
#
# Software distributed under the License is distributed on an AS IS basis,
# WITHOUT WARRANTY OF ANY KIND, either express or implied.  See the License
# for the specific language governing rights and limitations under the
# License.

# Written by Bram Cohen

from BitTorrent.CurrentRateMeasure import Measure
from BitTorrent.platform import bttime

#There is one object per remote peer. 
#This class manage the machinery to upload to peers.
#In particular this class maintains if the peers are interested or not in what
#I have. Also it maintains the request for pieces/blocks (index, begin, length)
#from the peers in the buffer[] list.
class Upload(object):

    def __init__(self, connection, ratelimiter, totalup, totalup2, choker,
                 storage, max_slice_length, max_rate_period, logcollector):
        self.connection = connection
        self.ratelimiter = ratelimiter
        self.totalup = totalup
        self.totalup2 = totalup2
        self.choker = choker
        self.storage = storage
        self.max_slice_length = max_slice_length
        self.max_rate_period = max_rate_period
        self.choked = True
        self.unchoke_time = None
        self.interested = False
        #the list buffer contains tuples (index, begin, lenght) for each
        #block requested by the remote peer. A non empty buffer means that
        #there is data to send to the remote peer already requested by the
        #remote peer. 
        self.buffer = []
        #PFS begin
        self.config = choker.config
        self.I = {}     # I[piece id] = block uploaded count in the piece id
        self.r = {}     # r[piece_id] = block requested count in the piece id
        #PFS end
        self.measure = Measure(max_rate_period)
        #send the bittfield of the peer the first time it connects to the peers. 
        if storage.do_I_have_anything():
            connection.send_bitfield(storage.get_have_list())
        self.logcollector = logcollector

    def got_not_interested(self):
        if self.interested:
            self.logcollector.log(None, 'R NI ' + str(self.connection.ip))
            self.interested = False
            del self.buffer[:]
            self.choker.not_interested(self.connection)

    def got_interested(self):
        if not self.interested:
            self.logcollector.log(None, 'R I ' + str(self.connection.ip))
            self.interested = True
            self.choker.interested(self.connection)

    def get_upload_chunk(self):
        if not self.buffer:
            return None
        #buffer.pop(0) return the element with index 0 and remove
        #this element from buffer.
        index, begin, length = self.buffer.pop(0)

        #PFS begin
        if self.choker.done():
            if index in self.I:
                self.I[index] += 1
            else:
                self.I[index] = 1
                if index in self.choker.I:
                    self.choker.I[index] += 1
                else:
                    self.choker.I[index] = 1
                self.logcollector.log(None, 'PFS ' + str(self.connection.ip) + \
                                      ' theta(' + str(index) + ') ' + str(self.choker.I[index]))
            if index not in self.choker.theta:
                self.choker.theta[index] = 1.0
        #PFS end

        piece = self.storage.get_piece(index, begin, length)
        if piece is None:
            self.logcollector.log(None, 'CON C ' + str(self.connection.ip) +  ' E 1')
            self.connection.close()
            return None
        self.measure.update_rate(len(piece))
        self.totalup.update_rate(len(piece))
        self.totalup2.update_rate(len(piece))
        return (index, begin, piece)

    def got_request(self, index, begin, length):
        if not self.interested or length > self.max_slice_length:
            self.logcollector.log(None, 'CON C ' + str(self.connection.ip) +  ' E 2')
            self.connection.close()
            return
        self.logcollector.log(None, 'R R ' + str(self.connection.ip) + ' i ' + str(index) + ' b ' + str(begin) + \
                              ' l ' + str(length))            
        if not self.connection.choke_sent:
            self.buffer.append((index, begin, length))
            if self.connection.next_upload is None and \
                   self.connection.connection.is_flushed():
                self.ratelimiter.queue(self.connection)

            # EPFS begin
            if self.choker.done():
                # update vector of requests {r1,...}
                self.PFS_update_r(index)
            # EPFS end


    # EPFS step 5: Seed updates his data structure when receiving REQUEST from leechers
    def PFS_update_r(self, index):
        if self.config['scheduling_algorithm'] == 'BT':
            return False
        if self.choker.tm_first_req == 0:
            self.choker.tm_first_req = bttime()
        if index in self.r:
            self.r[index] += 1
        else:
            self.r[index] = 1
            if index in self.choker.r:
                self.choker.r[index] += 1.0
            else:
                self.choker.r[index] = 1.0
                self.logcollector.log(None, 'PFS ' + str(self.connection.ip) + \
                                      ' r[' + str(index) + '] ' + str(self.choker.r[index]))
        return True
    # EPFS end

    def got_cancel(self, index, begin, length):
        try:
            self.buffer.remove((index, begin, length))
        except ValueError:
            pass

    def choke(self):
        if not self.choked:
            self.choked = True
            self.connection.send_choke()

    def sent_choke(self):
        assert self.choked
        del self.buffer[:]

    def unchoke(self, time):
        if self.choked:
            self.choked = False
            self.unchoke_time = time
            self.connection.send_unchoke()

    def has_queries(self):
        return len(self.buffer) > 0

    def get_rate(self):
        return self.measure.get_rate()

class EWMA(object):
    def __init__( self, alpha, init_avg = None ):
        """Exponentially Weighted Moving Average (EWMA) functor.
           @param alpha: the weight used in the EWMA using 'smaller is
              slower' convention.
           @param init_avg: starting value of the average.  If None then
              the average is only defined after the first sample and in
              the first call is set to the sample.

           """
        self._alpha = alpha
        self._avg = init_avg

    def __call__( self, sample = None ):
        """Computes the moving average after taking into account the passed
           sample.  If passed nothing then it just returns the current average
           value."""
        if sample == None:
            if self._avg == None:
                raise ValueError( "Tried to retrieve value from EWMA before "
                                  "first sample." )                
        else:
            if self._avg == None:
                self._avg = sample
            else:
                self._avg = (1-self._alpha) * self._avg + self._alpha * sample
        return self._avg

