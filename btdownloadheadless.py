#!/usr/bin/env python

# The contents of this file are subject to the BitTorrent Open Source License
# Version 1.0 (the License).  You may not copy or use this file, in either
# source code or executable form, except in compliance with the License.  You
# may obtain a copy of the License at http://www.bittorrent.com/license/.
#
# Software distributed under the License is distributed on an AS IS basis,
# WITHOUT WARRANTY OF ANY KIND, either express or implied.  See the License
# for the specific language governing rights and limitations under the
# License.

# Written by Bram Cohen, Uoti Urpala and John Hoffman

from __future__ import division

import sys
import os
import errno
import threading
from time import time, strftime
from signal import signal#, SIGWINCH
from cStringIO import StringIO

from BitTorrent.download import Feedback, Multitorrent
from BitTorrent.defaultargs import get_defaults
from BitTorrent.parseargs import parseargs, printHelp
from BitTorrent.zurllib import urlopen
from BitTorrent.bencode import bdecode
from BitTorrent.ConvertedMetainfo import ConvertedMetainfo
from BitTorrent import configfile
from BitTorrent import BTFailure
from BitTorrent import version

from datetime import datetime

#Instantiation of the LogCollector
from BitTorrent.LogCollector import LogCollector
## print 'logfile' + str(datetime.now().isoformat()) + '.log'
## try:
##     os.mkdir('./logs')
## except OSError, e:
##     # Ignore directory exists error
##     if e.errno <> errno.EEXIST:
##         raise
## logfile = open('./logs/logfile' + str(datetime.now().strftime('%Y%m%d%H%M%S')) + '.log', 'w')
## #logfile = sys.stdout
## logcollector = LogCollector(logfile)
logcollector = None

def fmttime(n):
    if n == 0:
        return 'download complete!'
    try:
        n = int(n)
        assert n >= 0 and n < 5184000  # 60 days
    except:
        return '<unknown>'
    m, s = divmod(n, 60)
    h, m = divmod(m, 60)
    return 'finishing in %d:%02d:%02d' % (h, m, s)

def fmtsize(n):
    s = str(n)
    size = s[-3:]
    while len(s) > 3:
        s = s[:-3]
        size = '%s,%s' % (s[-3:], size)
    if n > 999:
        unit = ['B', 'KiB', 'MiB', 'GiB', 'TiB', 'PiB', 'EiB', 'ZiB', 'YiB']
        i = 1
        while i + 1 < len(unit) and (n >> 10) >= 999:
            i += 1
            n >>= 10
        n /= (1 << 10)
        size = '%s (%.0f %s)' % (size, n, unit[i])
    return size


class HeadlessDisplayer(object):

    def __init__(self, doneflag):
        self.doneflag = doneflag

        self.done = False
        self.percentDone = ''
        self.timeEst = ''
        self.downRate = '---'
        self.upRate = '---'
        self.shareRating = ''
        self.seedStatus = ''
        self.peerStatus = ''
        self.errors = []
        self.file = ''
        self.downloadTo = ''
        self.fileSize = ''
        self.numpieces = 0

    def set_torrent_values(self, name, path, size, numpieces):
        self.file = name
        self.downloadTo = path
        self.fileSize = fmtsize(size)
        self.numpieces = numpieces

    def finished(self):
        self.done = True
        self.downRate = '---'
        self.display({'activity':'download succeeded', 'fractionDone':1})

    def error(self, errormsg):
        newerrmsg = strftime('[%H:%M:%S] ') + errormsg
        self.errors.append(newerrmsg)
        self.display({})

    def display(self, statistics):
        # statistics is a dictionary
        fractionDone = statistics.get('fractionDone')
        activity = statistics.get('activity')
        timeEst = statistics.get('timeEst')
        downRate = statistics.get('downRate')
        upRate = statistics.get('upRate')
        spew = statistics.get('spew')

        print '\n\n\n\n'

        #test for diagnostic mode
        if spew is not None:
            self.print_spew(spew)

        if timeEst is not None:
            self.timeEst = fmttime(timeEst)
        elif activity is not None:
            self.timeEst = activity

        #uprate and downrate are maintained using CurrentRateMeasure
        #they are updated using update_rate() in Downloader.py and Uploader.py
        if fractionDone is not None:
            self.percentDone = str(int(fractionDone * 1000) / 10)
        if downRate is not None:
            self.downRate = '%.1f KB/s' % (downRate / (1 << 10))
        if upRate is not None:
            self.upRate = '%.1f KB/s' % (upRate / (1 << 10))
        downTotal = statistics.get('downTotal')
        if downTotal is not None:
            upTotal = statistics['upTotal']
            if downTotal <= upTotal / 100:
                self.shareRating = 'oo  (%.1f MB up / %.1f MB down)' % (
                    upTotal / (1<<20), downTotal / (1<<20))
            else:
                self.shareRating = '%.3f  (%.1f MB up / %.1f MB down)' % (
                   upTotal / downTotal, upTotal / (1<<20), downTotal / (1<<20))

            numCopies = statistics['numCopies']
            nextCopies = ', '.join(["%d:%.1f%%" % (a,int(b*1000)/10) for a,b in
                    zip(xrange(numCopies+1, 1000), statistics['numCopyList'])])
            if not self.done:
                self.seedStatus = '%d seen now, plus %d distributed copies ' \
                                  '(%s)' % (statistics['numSeeds'],
                                         statistics['numCopies'], nextCopies)
            else:
                self.seedStatus = '%d distributed copies (next: %s)' % (
                    statistics['numCopies'], nextCopies)
            self.peerStatus = '%d seen now' % statistics['numPeers']

        for err in self.errors:
            print 'ERROR:\n' + err + '\n'

        print 'saving:        ', self.file
        print 'percent done:  ', self.percentDone
        print 'time left:     ', self.timeEst
        print 'download to:   ', self.downloadTo
        print 'download rate: ', self.downRate
        print 'upload rate:   ', self.upRate
        print 'share rating:  ', self.shareRating
        print 'seed status:   ', self.seedStatus
        print 'peer status:   ', self.peerStatus
        print 'scheduling:    ', config['scheduling_algorithm']
        print 'interval time  ', config['unchoke_interval']
        logcollector.log(None, 'GS ' + \
                         ' P ' + str(self.percentDone) + \
                         ' D ' + str(self.downRate) + \
                         ' U ' + str(self.upRate) +\
                         ' NS ' + str(statistics.get('numSeeds',0)) +\
                         ' NL ' + str(statistics.get('numPeers',0)) +\
                         ' SR ' + str( self.shareRating)\
                         )
                                  

    def print_spew(self, spew):
        #s = StringIO()
        #s.write('\n\n\n')
        for c in spew:
            s='%20s ' % c['ip']

            #l means that the connection was initiated locally by the client
            #r means that the connection was initiated remotely
            if c['initiation'] == 'L':
                s += ' l '
            else:
                s += ' r '
                
            #meaning of the upload keyword
            total, rate, interested, choked = c['upload']
            s += ' %10s %10s ' % (str(int(total/10485.76)/100),
                                     str(int(rate)))
            if c['is_optimistic_unchoke']:
                s += ' * '
            else:
                s += '   '
            if interested:
                s += ' i '
            else:
                s += '   '
            if choked:
                s += ' c '
            else:
                s += '   '

            #meaning of the download keyword
            total, rate, interested, choked, snubbed = c['download']
            s += ' %10s %10s ' % (str(int(total/10485.76)/100),
                                     str(int(rate)))
            if interested:
                s += ' i '
            else:
                s += '   '
            if choked:
                s += ' c '
            else:
                s += '   '
            if snubbed:
                s += ' s '
            else:
                s += '   ' 
            logcollector.log(s)


class DL(Feedback):

    def __init__(self, metainfo, config):
        self.doneflag = threading.Event()
        self.metainfo = metainfo
        self.config = config
        self.init_logfile()
        self.show_config()

    def init_logfile(self):
        global logcollector
        print 'logfile' + str(datetime.now().isoformat()) + '.log'
        try:
            os.mkdir('./logs')
        except OSError, e:
            # Ignore directory exists error
            if e.errno <> errno.EEXIST:
                raise
        logfile = open(('./logs/%s_%s_i%d_a%.2f_sd%d_su%d_logfile' % ( \
                                                   self.config['test_id'], \
                                                   self.config['scheduling_algorithm'], \
                                                   self.config['unchoke_interval'],
                                                   self.config['EPFS_alpha'],
                                                   self.config['shutdown_after_seed'],
                                                   self.config['simultaneous_upload'])) + \
                       str(datetime.now().strftime('%Y%m%d%H%M%S')) + '.log', 'w')
        #logfile = sys.stdout
        logcollector = LogCollector(logfile)

        
        logcollector.log(None, 'L   ****************************LEGEND (Begin)**********************')

        #btdownloadheadless.py
        logcollector.log(None,'L   (btdownloadheadless.py) GS P <percent> D <downrate> U <uprate> NS <ns> NL <nl> SR <sharerate>: General statistics\
        on the session. The peer has completed <percent> % of the content, the aggregate download rate is <downrate> Bytes/s,\
        the aggregate upload rate is <uprate> Bytes/s, the number of seeds (handshake done) is <ns>,\
        the number of leechers (handshake done) is <nl>, and the sharing rate is <sharerate>')        
        logcollector.log(None,'L   (btdownloadheadless.py) FN <name> FZ <size> NP <numpieces> PL <piecelength>: The torrent file name is <name> with a size <size> and a number of pieces <numpieces> of length <piecelength>')
        logcollector.log(None,'L   (btdownloadheadless.py) CF: Important config parameters')
        
        #Choker.py
        logcollector.log(None,'L   (Choker.py) PRU <IP>: peer in leecher state performs a regular unchoke of the peer with IP <IP>')
        logcollector.log(None,'L   (Choker.py) SRU <IP>: peer in seed state performs a regular unchoke of the peer with IP <IP>')
        logcollector.log(None,'L   (Choker.py) SKU <IP>: peer in seed state keeps unchoked the peer with IP <IP>')        
        logcollector.log(None,'L   (Choker.py) POU I <IP>: peer in leecher state peforms an optimisitic unchoke of the interested peer with IP <IP>')
        logcollector.log(None,'L   (Choker.py) POU NI <IP>: peer in leecher state peforms an optimistic unchoke of the not interested peer with IP <IP>')
        logcollector.log(None,'L   (Choker.py) PPOU <IP>: peer in leecher state planned optimistic unchoke with IP <IP>')
        logcollector.log(None,'L   (Choker.py) SB <IP>: The peer with IP <IP> is snubbed')
        logcollector.log(None,'L   (Choker.py) INT LP <ins> PC <ru> C <ou> R <r>.: Choker.py internal values. <ins> is the number\
        of peers interested and not snubbed, <ru>+1 is the number of regular unchoke to perform, \
        <ou> is the number of optimistic unchoke to perform, the _round_robin method was called <r> times.')
        logcollector.log(None,'L   (Choker.py) GSP <IP> M <state> R <round> U <uprate> UB <uptotal> D <downrate> DB <downtotal>: \
        General statistics for the peer with IP <IP>. The peer is a seed if <state> is S, a leecher if <state> is L\
        The round in _round_robin is <round> The local peer currently uploads at a rate <uprate> Bytes/s to the peer with IP <IP> and  \
        downloads at a rate <downrate> Bytes/s from the peer with IP <IP>. The aggregated amount of bytes uploaded (resp. downloaded) is \
        <uptotal> (resp. <downtotal>)')
        logcollector.log(None,'L   (Choker.py) INT i <unchokes>: Choker.py internal values. <unchokes>  is the maximum\
        number of unchokes to perform per 30 seconds periods')
        logcollector.log(None,'L   (Choker.py) INT NFU <nfu> NL <nl> NK <nk> NN <nn> USL <usl> R <r>: Choker.py internal values.\
        <nfu> is the number of unchokes to perform on a 10 seconds interval, \
        <nl> is the memory for peer selection, \
        <nk> is the number of connections to keep, <nn> is the number of connections that will be choked, \
        <usl> is the number of unchokes since the last regular call to _rechoke_seed, the _round_robin method was called <r> times.')
        
        #Encoder.py
        logcollector.log(None,'L   (Encoder.py) CONH L <IP>: Handshake performed for the connection locally initiated to peer with IP <IP>')
        logcollector.log(None,'L   (Encoder.py) CONH R <IP>: Handshake performed for the connection remotely initiated from peer with IP <IP>')
        logcollector.log(None,'L   (Encoder.py) CON ST <IP>: try to establish a socket connection to the peer with IP <IP>, Socket Try')
        logcollector.log(None,'L   (Encoder.py) CON SE <IP>: the socket connection to the peer with IP <IP> has failed, Socket Exception')
        logcollector.log(None,'L   (Encoder.py) CON SS <IP>: the socket connection to the peer with IP <IP> has succeeded, Socket Success')        
        
        #Connecter.py
        #CON simply means that the network connection was initiated. There is no BitTorrent handshake yet.
        logcollector.log(None,'L   (Connecter.py) CON L <IP>: Connection locally initiated to peer with IP <IP>')        
        logcollector.log(None,'L   (Connecter.py) CON R <IP>: Connection remotely initiated from peer with IP <IP>')
        logcollector.log(None,'L   (Connecter.py) S I <IP>: This peer has sent a INTERESTED messsage to peer with IP <IP>')
        logcollector.log(None,'L   (Connecter.py) S NI <IP>: This peer has sent a NOT_INTERESTED messsage to peer with IP <IP>')
        logcollector.log(None,'L   (Connecter.py) S C <IP>: This peer has sent a CHOKE messsage to peer with IP <IP>')
        logcollector.log(None,'L   (Connecter.py) S UC <IP>: This peer has sent a UNCHOKE messsage to peer with IP <IP>')
        logcollector.log(None,'L   (Connecter.py) S R <IP> i <index> b <begin> l <length>: This peer has sent a REQUEST \
        messsage to peer with IP <IP>, the index of the piece is <index>, the beginning (offset) wihtin the piece is <begin> and the\
        requested block length is <length>')
        logcollector.log(None,'L   (Connecter.py) S P <IP> i <index> b <begin> l <length>: This peer has sent a PIECE \
        messsage to peer with IP <IP>, the index of the piece is <index>, the beginning (offset) within the piece is <begin> and the\
        block length is <length>')
        logcollector.log(None,'L   (Connecter.py) S CA <IP> i <index> b <begin> l <length>: This peer has sent a CANCEL \
        messsage to peer with IP <IP>, the index of the piece is <index>, the beginning (offset) within the piece is <begin>\
        and the block length is <length>')
        logcollector.log(None,'L   (Connecter.py) S H <IP> i <index>: This peer has sent a HAVE \
        messsage to peer with IP <IP> for the piece with index <index>')
        logcollector.log(None,'L   (Connecter.py) S KA <IP>: This peer has sent a KEEP_ALIVE message to peer with IP <IP>')
        logcollector.log(None,'L   (Connecter.py) R KA <IP>: This peer has received a KEEP_ALIVE message from peer with IP <IP>')
        logcollector.log(None,'L   (Connecter.py) R CA <IP> i <index> b <begin> l <length>: This peer has received a CANCEL \
        messsage from peer with IP <IP>, the index of the piece is <index>, the beginning (offset) within the piece is <begin>\
        and the block length is <length>')
        logcollector.log(None,'L   (Connecter.py) CON C <IP> E <i> : The connection with the peer with IP <IP> was closed due to an error.\
        <i> in {3-15} is the error ID. Seek in Connecter.py for the meaning of this ID')
        logcollector.log(None,'L   (Connecter.py) CONH C <IP>: A completed (i.e., handshake done) connection with peer with IP <IP> was closed')

        #Uploader.py
        logcollector.log(None,'L   (Uploader.py) R I <IP>: This peer has received a INTERESTED messsage from peer with IP <IP>')
        logcollector.log(None,'L   (Uploader.py) R NI <IP>: This peer has received a NOT_INTERESTED messsage from peer with IP <IP>')
        logcollector.log(None,'L   (Uploader.py) R R <IP> i <index> b <begin> l <length>: This peer has received a REQUEST \
        messsage from peer with IP <IP>, the index of the piece is <index>, the beginning (offset) wihtin the piece is <begin> and the \
        requested block length is <length>')
        logcollector.log(None,'L   (Uploader.py) CON C <IP> E <i>: The connection with the peer with IP <IP> was closed due to an error.\
        <i> in {1,2} is the error ID. Seek in Uploader.py for the meaning of this ID')
        
        #Downloader.py
        logcollector.log(None,'L   (Downloader.py) R C <IP>: This peer has received a CHOKE messsage from peer with IP <IP>')
        logcollector.log(None,'L   (Downloader.py) R UC <IP>: This peer has received a UNCHOKE messsage from peer with IP <IP>')
        logcollector.log(None,'L   (Downloader.py) R H <IP> i <index>: This peer has received a HAVE \
        messsage from peer with IP <IP> for the piece with index <index>')
        logcollector.log(None,'L   (Downloader.py) R BF <IP> <bitfield>: This peer has received the BITFIELD <bitfield> \
        from peer with IP <IP>')
        #Here I only report a peer as a seed when its bittfied message identifies it as a seed, i.e., at most after each connection
        #handshake validated. I do not report here leechers that became a seed during a single connection.
        #This change of state is logged in the general stats of a peer (GSP).
        logcollector.log(None,'L   (Downloader.py) P <IP> S: The peer with IP <IP> is a seed when he join the peer set. That is, its initial bitfield is full. This message is not displayed when a remote peer become a seed while connected to the local peer.')
        logcollector.log(None,'L   (Downloader.py) CON C <IP> S: The connection with the peer with IP <IP> was closed, because the\
        local peer is a seed and the remote peer with IP <IP> is also a seed.')
        logcollector.log(None,'L   (Downloader.py) R P <IP> i <index> b <begin>: This peer has received a PIECE \
        from peer with IP <IP>, the index of the piece is <index>, the beginning (offset) within the piece is <begin>')

        #Rerequester.py
        logcollector.log(None,'L   (Rerequester.py) RT NL <n_leechers> NS <n_seeds>: The peer received from the tracker that \
        there are <n_leechers> leechers and <n_seeds> seeds in the torrent (equivallent to scrape mode)')
        logcollector.log(None,'L   (Rerequester.py) RT ID <id> AI <interval>: The peer received from the tracker the tracker \
        ID <id> and the tracker regular request interval <interval> in seconds')
        logcollector.log(None,'L   (Rerequester.py) RT NP <n_peers>: The peer received from the tracker a list containing <n_peers> peers')
        logcollector.log(None,'L   (Rerequester.py) RT P <IP> <port> <peer_id>: The peer received from the tracker\
        the coordinate of a peer with IP <IP> port <port>  and ID <peer_id>')        

        #download.py
        logcollector.log(None,'L   (download.py) P ID <id>: The peer ID is <id>')
        logcollector.log(None,'L   (download.py) P SM: The peer has downloaded the content, it switches to seed state')

        #StorageWrapper.py
        logcollector.log(None,'L   (StorageWrapper.py) P EG: The peer has switched to end game mode')

        logcollector.log(None, 'L   ****************************LEGEND (End)**********************')                

    def show_config(self):
        logcollector.log(None, 'CF rerequest_interval ' + str(config.get('rerequest_interval',None))+ ' seconds '+\
                               ' min_peers ' + str(config.get('min_peers',None))+\
                               ' max_initiate ' + str(config.get('max_initiate',None))+\
                               ' max_allow_in ' + str(config.get('max_allow_in',None))+\
                               ' download_slice_size(blockSize) ' + str(config.get('download_slice_size', None)) +\
                               ' max_upload_rate ' + str(config.get('max_upload_rate',None))+ ' kB/s ' +\
                               ' max_uploads ' + str(config.get('max_uploads',None))
                         )
                
    def run(self):
        #definition of the displayer self.d
        self.d = HeadlessDisplayer(self.doneflag)
        try:
            self.multitorrent = Multitorrent(self.config, self.doneflag,
                                             self.global_error, logcollector)
            # raises BTFailure if bad
            metainfo = ConvertedMetainfo(bdecode(self.metainfo))
            torrent_name = metainfo.name_fs
            if config['save_as']:
                if config['save_in']:
                    raise BTFailure('You cannot specify both --save_as and '
                                    '--save_in')
                saveas = config['save_as']
            elif config['save_in']:
                saveas = os.path.join(config['save_in'], torrent_name)
            else:
                saveas = torrent_name

            #set torrent information in the displayer
            self.d.set_torrent_values(metainfo.name, os.path.abspath(saveas),
                                metainfo.total_bytes, len(metainfo.hashes))
            logcollector.log(None, 'FN ' +  str(metainfo.name).replace(' ', '_') + ' FZ ' + str(metainfo.total_bytes)+\
                             ' NP ' + str(len(metainfo.hashes)) + ' PL ' + str(metainfo.piece_length))
            
            #start the BitTorrent protocol
            #pass to the torrent object the DL object used for statistic displaying
            self.torrent = self.multitorrent.start_torrent(metainfo,
                                self.config, self, saveas)
        except BTFailure, e:
            print str(e)
            return
        self.get_status()
        #It only returns when the application is stopped
        self.multitorrent.rawserver.listen_forever()

        #graceful shutdown. We reach this section when the listen_forever() is stopped.
        self.d.display({'activity':'shutting down', 'fractionDone':0})
        self.torrent.shutdown()

    def reread_config(self):
        try:
            newvalues = configfile.get_config(self.config, 'btdownloadcurses')
        except Exception, e:
            self.d.error('Error reading config: ' + str(e))
            return
        self.config.update(newvalues)
        # The set_option call can potentially trigger something that kills
        # the torrent (when writing this the only possibility is a change in
        # max_files_open causing an IOError while closing files), and so
        # the self.failed() callback can run during this loop.
        for option, value in newvalues.iteritems():
            self.multitorrent.set_option(option, value)
        for option, value in newvalues.iteritems():
            self.torrent.set_option(option, value)

    def get_status(self):
        self.multitorrent.rawserver.add_task(self.get_status,
                                             self.config['display_interval'])
        status = self.torrent.get_status(self.config['spew'])
        self.d.display(status)

    def global_error(self, level, text):
        self.d.error(text)

    def error(self, torrent, level, text):
        self.d.error(text)

    def failed(self, torrent, is_external):
        self.doneflag.set()

    def finished(self, torrent):
        self.d.finished()


if __name__ == '__main__':
    uiname = 'btdownloadheadless'
    defaults = get_defaults(uiname)

    if len(sys.argv) <= 1:
        printHelp(uiname, defaults)
        sys.exit(1)
    try:
        config, args = configfile.parse_configuration_and_args(defaults,
                                      uiname, sys.argv[1:], 0, 1)
        if args:
            if config['responsefile']:
                raise BTFailure, 'must have responsefile as arg or ' \
                      'parameter, not both'
            config['responsefile'] = args[0]
        try:
            if config['responsefile']:
                h = file(config['responsefile'], 'rb')
                metainfo = h.read()
                h.close()
            elif config['url']:
                h = urlopen(config['url'])
                metainfo = h.read()
                h.close()
            else:
                raise BTFailure('you need to specify a .torrent file')
        except IOError, e:
            raise BTFailure('Error reading .torrent file: ', str(e))
    except BTFailure, e:
        print str(e)
        sys.exit(1)

    dl = DL(metainfo, config)
    dl.run()
