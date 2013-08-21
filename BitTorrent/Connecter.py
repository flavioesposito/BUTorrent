# The contents of this file are subject to the BitTorrent Open Source License
# Version 1.0 (the License).  You may not copy or use this file, in either
# source code or executable form, except in compliance with the License.  You
# may obtain a copy of the License at http://www.bittorrent.com/license/.
#
# Software distributed under the License is distributed on an AS IS basis,
# WITHOUT WARRANTY OF ANY KIND, either express or implied.  See the License
# for the specific language governing rights and limitations under the
# License.

# Originally written by Bram Cohen, heavily modified by Uoti Urpala

# required for python 2.2
from __future__ import generators

from binascii import b2a_hex

from BitTorrent.bitfield import Bitfield
from BitTorrent.obsoletepythonsupport import *

def toint(s):
    return int(b2a_hex(s), 16)

def tobinary(i):
    return (chr(i >> 24) + chr((i >> 16) & 0xFF) +
        chr((i >> 8) & 0xFF) + chr(i & 0xFF))

CHOKE = chr(0)
UNCHOKE = chr(1)
INTERESTED = chr(2)
NOT_INTERESTED = chr(3)
# index
HAVE = chr(4)
# index, bitfield
BITFIELD = chr(5)
# index, begin, length
REQUEST = chr(6)
# index, begin, piece
PIECE = chr(7)
# index, begin, piece
CANCEL = chr(8)

protocol_name = 'BitTorrent protocol'

#manage all the messages received and sent.
#there is one object per connection
class Connection(object):

    def __init__(self, encoder, connection, id, is_local, logcollector):
        self.encoder = encoder
        self.connection = connection
        self.id = id
        self.ip = connection.ip
        self.locally_initiated = is_local
        self.complete = False
        self.closed = False
        self.got_anything = False
        self.next_upload = None
        self.upload = None
        self.download = None
        self._buffer = []
        self._buffer_len = 0
        self._reader = self._read_messages()
        self._next_len = self._reader.next()
        self._partial_message = None
        self._outqueue = []
        self.choke_sent = True
        self.logcollector=logcollector
        if self.locally_initiated:
            self.logcollector.log(None, 'CON L ' + str(self.ip))
        else:
            self.logcollector.log(None, 'CON R ' + str(self.ip))
        if self.locally_initiated:
            connection.write(chr(len(protocol_name)) + protocol_name +
                (chr(0) * 8) + self.encoder.download_id)
            if self.id is not None:
                connection.write(self.encoder.my_id)

    def close(self):
        if not self.closed:
            self.connection.close()
            self._sever()

    def send_interested(self):
        self.logcollector.log(None, 'S I ' + str(self.ip))
        self._send_message(INTERESTED)

    def send_not_interested(self):
        self.logcollector.log(None, 'S NI ' + str(self.ip))
        self._send_message(NOT_INTERESTED)

    def send_choke(self):
        if self._partial_message is None:
            self.logcollector.log(None, 'S C ' + str(self.ip))
            self._send_message(CHOKE)
            self.choke_sent = True
            self.upload.sent_choke()

    def send_unchoke(self):
        if self._partial_message is None:
            self.logcollector.log(None, 'S UC ' + str(self.ip))
            self._send_message(UNCHOKE)
            self.choke_sent = False

    def send_request(self, index, begin, length):
        self.logcollector.log(None, 'S R ' + str(self.ip) +  ' i ' + str(index) + ' b ' + str(begin) + ' l ' + str(length))
        self._send_message(REQUEST + tobinary(index) +
            tobinary(begin) + tobinary(length))

    def send_cancel(self, index, begin, length):
        self.logcollector.log(None, 'S CA ' + str(self.ip) +  ' i ' + str(index) + ' b ' + str(begin) + ' l ' + str(length))
        self._send_message(CANCEL + tobinary(index) +
            tobinary(begin) + tobinary(length))

    def send_bitfield(self, bitfield):
        self.logcollector.log(None, 'S BF ' + str(self.ip))
        self._send_message(BITFIELD + bitfield)

    def send_have(self, index):
        self.logcollector.log(None, 'S H ' + str(self.ip) +  ' i ' + str(index))        
        self._send_message(HAVE + tobinary(index))

    def send_keepalive(self):
        self.logcollector.log(None, 'S KA ' + str(self.ip))        
        self._send_message('')

    #used by the RateLimiter to limit the upload rate
    #The RateLimiter calls send_partial each time there is a room to send
    #a block without exceeding the upload rate limit.
    #the bytes parameter is constant for the entire session, it is set by default to 1380 Bytes.
    #as the block size is 16384 bytes by default, there is 12 packets sent in the network
    #per block.
    def send_partial(self, bytes):
        if self.closed:
            return 0
        if self._partial_message is None:
            #get_upload_chunk returns a block to send
            s = self.upload.get_upload_chunk()
            if s is None:
                return 0
            index, begin, piece = s
            #when this log is sent, there is no guarantee that the block was entirely sent to the peer.
            #Indeed, if the block is too large, it will be sent in several _partial_messages
            self.logcollector.log(None, 'S P ' + str(self.ip) +  ' i ' + str(index) + ' b ' + str(begin) + ' l ' + str(len(piece)))
            #''.join(list) converts the list to a string
            self._partial_message = ''.join((tobinary(len(piece) + 9), PIECE,
                                    tobinary(index), tobinary(begin), piece))
        if bytes < len(self._partial_message):
            #buffer(S,b,l) returns a new buffer object containing a substring
            #of S starting to index b finishing to b+l. l is optional
            self.connection.write(buffer(self._partial_message, 0, bytes))
            self._partial_message = buffer(self._partial_message, bytes)
            return bytes

        queue = [str(self._partial_message)]
        self._partial_message = None
        #when a message is still being sent (_partial_message not None),
        #CHOKE and UNCHOKE messages are delayed. They are evantually
        #sent in the following lines that are equivallent to _send_message()
        if self.choke_sent != self.upload.choked:
            if self.upload.choked:
                self.logcollector.log(None, 'S C ' + str(self.ip))
                self._outqueue.append(tobinary(1) + CHOKE)
                self.upload.sent_choke()
            else:
                self.logcollector.log(None, 'S UC ' + str(self.ip))
                self._outqueue.append(tobinary(1) + UNCHOKE)
            self.choke_sent = self.upload.choked
        queue.extend(self._outqueue)
        self._outqueue = []
        queue = ''.join(queue)
        self.connection.write(queue)
        return len(queue)

    # yields the number of bytes it wants next, gets those in self._message
    def _read_messages(self):
        # First entered for a handshake message to check its correct format
        #1+19(actual header length)+8+20+20
        yield 1   # header length
        if ord(self._message) != len(protocol_name):
            return

        yield len(protocol_name) #should be 19
        if self._message != protocol_name:
            return

        yield 8  # reserved

        yield 20 # download id
        if self.encoder.download_id is None:  # incoming connection
            # modifies self.encoder if successful
            self.encoder.select_torrent(self, self._message)
            if self.encoder.download_id is None:
                return
        elif self._message != self.encoder.download_id:
            return
        if not self.locally_initiated:
            self.connection.write(chr(len(protocol_name)) + protocol_name +
                (chr(0) * 8) + self.encoder.download_id + self.encoder.my_id)

        yield 20  # peer id
        if not self.id:
            self.id = self._message
            if self.id == self.encoder.my_id:
                return
            for v in self.encoder.connections.itervalues():
                if v is not self:
                    if v.id == self.id:
                        return
                    if self.encoder.config['one_connection_per_ip'] and \
                           v.ip == self.ip:
                        return
            if self.locally_initiated:
                self.connection.write(self.encoder.my_id)
            else:
                self.encoder.everinc = True
        else:
            if self._message != self.id:
                return
        self.complete = True
        self.encoder.connection_completed(self)

        #reaches the while loop when the handshake is validated. The While loop
        #checks the format of all the messages after the handshake. 
        while True:
            yield 4   # message length
            l = toint(self._message)
            if l > self.encoder.config['max_message_length']:
                return
            if l > 0:
                yield l
                self._got_message(self._message)
            else:
                self.logcollector.log(None, 'R KA ' +  str(self.ip))

    #this method is called by _read_message when the handshake is done.
    #The CON C in this method can only happen on a completed (i.e., handshake done)
    #connection
    #All the messages passed to this method have the length prefix (4 bytes) already removed
    def _got_message(self, message):
        #message ID is a single decimal char. 
        t = message[0]
        #close the connection if the Bitfield is received more than once.
        if t == BITFIELD and self.got_anything:
            self.logcollector.log(None, 'CON C ' +  str(self.ip) + ' E 3')
            self.close()
            return
        self.got_anything = True
        #all these messages must be a single byte: the message ID
        if (t in [CHOKE, UNCHOKE, INTERESTED, NOT_INTERESTED] and
                len(message) != 1):
            self.logcollector.log(None, 'CON C ' +  str(self.ip) + ' E 4')            
            self.close()
            return
        if t == CHOKE:
            #log set in Downloader.py in order to avoid logging duplicate messages
            #such messages are implementation error or DoS, but not part of the protocol
            self.download.got_choke()
        elif t == UNCHOKE:
            #log set in Downloader.py in order to avoid logging duplicate messages
            #such messages are implementation error or DoS, but not part of the protocol
            self.download.got_unchoke()
        elif t == INTERESTED:
            #log set in Downloader.py in order to avoid logging duplicate messages
            #such messages are implementation error or DoS, but not part of the protocol
            self.upload.got_interested()
        elif t == NOT_INTERESTED:
            #log set in Downloader.py in order to avoid logging duplicate messages
            #such messages are implementation error or DoS, but not part of the protocol
            self.upload.got_not_interested()
        #The HAVE message must be 5 bytes long
        elif t == HAVE:
            if len(message) != 5:
                self.logcollector.log(None, 'CON C ' +  str(self.connection.ip) + ' E 5')                
                self.close()
                return
            i = toint(message[1:])
            if i >= self.encoder.numpieces:
                self.logcollector.log(None, 'CON C ' +  str(self.connection.ip) + ' E 6')                
                self.close()
                return
            #log set in Downloader.py in order to avoid logging duplicate messages
            #such messages are implementation error or DoS, but not part of the protocol
            self.download.got_have(i)
        elif t == BITFIELD:
            try:
                b = Bitfield(self.encoder.numpieces, message[1:])
            except ValueError:
                self.logcollector.log(None, 'CON C ' +  str(self.connection.ip) + ' E 7')
                self.close()
                return
            #log set in Downloader.py 
            self.download.got_have_bitfield(b)
        #The REQUEST message is 13 bytes long
        elif t == REQUEST:
            if len(message) != 13:
                self.logcollector.log(None, 'CON C ' +  str(self.connection.ip) + ' E 8')
                self.close()
                return
            i = toint(message[1:5])
            if i >= self.encoder.numpieces:
                self.logcollector.log(None, 'CON C ' +  str(self.connection.ip) + ' E 9')
                self.close()
                return
            #log set in Uploader.py in order to avoid logging duplicate messages
            #such messages are implementation error or DoS, but not part of the protocol
            self.upload.got_request(i, toint(message[5:9]),
                toint(message[9:]))
        #the CANCEL message is 13 bytes long
        elif t == CANCEL:
            if len(message) != 13:
                self.logcollector.log(None, 'CON C ' +  str(self.connection.ip) + ' E 10')
                self.close()
                return
            i = toint(message[1:5])
            if i >= self.encoder.numpieces:
                self.logcollector.log(None, 'CON C ' +  str(self.connection.ip) + ' E 11')                
                self.close()
                return
            self.logcollector.log(None, 'R CA ' + str(self.ip) + ' i ' + str(i) + ' b ' + str(toint(message[5:9])) + \
                                  ' l ' + str(toint(message[9:])))
            self.upload.got_cancel(i, toint(message[5:9]),
                toint(message[9:]))
        #The PIECE message header must be 9 bytes long. Close the connection is a
        #PIECE message is sent without payload. 
        elif t == PIECE:
            if len(message) <= 9:
                self.logcollector.log(None, 'CON C ' +  str(self.connection.ip) + ' E 12')
                self.close()
                return
            i = toint(message[1:5])
            if i >= self.encoder.numpieces:
                self.logcollector.log(None, 'CON C ' +  str(self.connection.ip) + ' E 13')
                self.close()
                return
            if self.download.got_piece(i, toint(message[5:9]), message[9:]):
                #Log set in Downloader.py in order to avoid logging not requested messages.
                #Send HAVE messages to all the peers for the received piece.
                #Each received block does not trigger the sent of a HAVE message
                #only full pieces trigger the sent of such a message.
                for co in self.encoder.complete_connections:
                    co.send_have(i)
        #message ID not recongnized.
        else:
            self.logcollector.log(None, 'CON C ' +  str(self.connection.ip) + ' E 14')
            self.close()

    #clears the state related to the connection when it is closed.
    def _sever(self):
        self.closed = True
        self._reader = None
        del self.encoder.connections[self.connection]
        self.encoder.replace_connection()
        if self.complete:
            self.logcollector.log(None,'CONH C ' + str(self.connection.ip))
            del self.encoder.complete_connections[self]
            self.download.disconnected()
            self.encoder.choker.connection_lost(self)
            self.upload = self.download = None

    def _send_message(self, message):
        s = tobinary(len(message)) + message
        if self._partial_message is not None:
            self._outqueue.append(s)
        else:
            self.connection.write(s)

    #Called by the RawServer.
    #can be called before the connection is completed. Thus CON C can happen
    #if the remote peer does not complete its handshake.
    def data_came_in(self, conn, s):
        while True:
            if self.closed:
                return
            i = self._next_len - self._buffer_len
            if i > len(s):
                self._buffer.append(s)
                self._buffer_len += len(s)
                return
            m = s[:i]
            if self._buffer_len > 0:
                self._buffer.append(m)
                m = ''.join(self._buffer)
                self._buffer = []
                self._buffer_len = 0
            s = s[i:]
            self._message = m
            try:
                self._next_len = self._reader.next()
            #StopIteration is raised by the next() iterator when there is no more values
            #This is a python built in Exception
            except StopIteration:
                self.logcollector.log(None, 'CON C ' +  str(self.connection.ip) + ' E 15')
                self.close()
                return

    #called by the RawServer._close_socket when the socket is closed
    #due to a timeout or a socket error
    #for a detailled reason of the socket close, a log can be performed
    #in RawServer.py before each call to _close_socket. 
    def connection_lost(self, conn):
        assert conn is self.connection
        self.logcollector.log(None, 'CON C ' +  str(self.connection.ip) + ' E 16')
        self._sever()

    def connection_flushed(self, connection):
        if self.complete:
            if self.next_upload is None and (self._partial_message is not None
                                             or self.upload.buffer):
                self.encoder.ratelimiter.queue(self)
