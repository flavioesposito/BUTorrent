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

from random import randrange
from BitTorrent.platform import bttime

#This class handles the choke algorithm
#the Uploader informs the Choker when a peer is interested or not
#the Encoder informs the Choker when a new connection is made
#The Choker object maintains a list (connections[]) of all the connections.
class Choker(object):

    def __init__(self, config, schedule, logcollector, done = lambda: False):
        self.config = config
        self.schedule = schedule
        self.connections = []
        self.count = 0
        self.done = done
        self.unchokes_since_last = 0
        self.logcollector = logcollector
        #PFS begin
        self.round = 1
        self.theta = {}  # theta[piece id] = Average Value
        self.r = {}      # r[piece id] = requested count
        self.I = {}      # I[piece id] = 1 or (0 or null)
        self.T = self.config['unchoke_interval']
        self.EPFS = self.config['scheduling_algorithm'] == 'EPFS'
        self.tm_first_req = 0   # time got the first request
        #PFS end
        schedule(self._round_robin, self.T)

    #Unchoke every 10 seconds and optimistic unchoke every 30 seconds
    def _round_robin(self):
        self.schedule(self._round_robin, self.T)
        self.count += 1

        #write every 10 seconds stats on the downrate and uprate of the peers.
        for i in xrange(len(self.connections)):
            c = self.connections[i]
            if c.download.have.numfalse == 0:
                mode='S'
            else:
                mode='L'
            self.logcollector.log(None,'GSP ' + str(c.ip) + ' M ' + mode +\
                                  ' R ' + str(self.count)+\
                                  ' U ' + str(c.upload.get_rate())+\
                                  ' UB ' + str(c.upload.measure.get_total()) + \
                                  ' D ' + str(c.download.get_rate()) + \
                                  ' DB ' + str(c.download.measure.get_total()))


        #If a seed then call the _rechoke_seed method
        if self.done():
            if self.config['scheduling_algorithm'] == 'BT': 
                self._rechoke_seed(True)
            else:
                # PFS step 1-3: unchoke every leecher
                self.PFS_unchoke_all()
                if not self.PFS_apply():
                    self._rechoke_seed()
            return
        
        #When a leecher, evaluates the peer to optimistically unchoke
        #every 30 seconds
        if self.count % 3 == 0:
            for i in xrange(len(self.connections)):
                u = self.connections[i].upload
                if u.choked and u.interested:
                    #If connections[i] is a peer choked and interested
                    #puts the connection i the first one in the list connections[]
                    #There is no optimistic unchoke flag. The optimisitic unchoke
                    #state is implicit (the first peer in connections[]).
                    #
                    #Several optimistic unchokes can be performed by the algorithm,
                    #(see _rechoke depending on Anti snubbing and min_uploads)
                    #but only one connection is elected by this part of the code as
                    #optimisitic unchoke every 30 seconds.
 
                    self.logcollector.log(None,'PPOU ' + str(self.connections[i].ip))
                    self.connections = self.connections[i:] + self.connections[:i]
                    break
        self._rechoke()


    # EPFS
    def PFS_unchoke_all(self):
        #step 1-3:
        #for all Leecher i in Seed's peerset do
        #    See UNCHOKES Leecher i in this peerset
        #end for
        _choks = []
        for i, c in enumerate(self.connections):
            u = c.upload
            if u.choked and u.interested:
                u.unchoke(self.count)
                _choks.append(c.ip)
        msg = 'PFS unchoke all ' + str(_choks)
        print msg
        self.logcollector.log(None, msg)

    # PFS step 6 - 13:
    def PFS_apply(self):
        #step 6 - 7:
        #calculate number of request collected
        reqs = len(self.r)

        #if (Number of request collected is <= 4) V (1 second has passed since I got the first request)
        SU = self.config['simultaneous_upload']
        if reqs <= SU:
            # to step 7: With a FCFC policy, Seed sends PIECE requested
            return False


        if (self.tm_first_req != 0) and (self.tm_first_req - bttime() > 1.0):
            msg = 'PFS ' + str(self.tm_first_req - bttime()) + ' second has passed since first request'
            print msg
            self.logcollector.log(None, msg)

            self.PFS_clear_structure()
            # to step 7: With a FCFC policy, Seed sends PIECE requested
            return False

        #log output
        msg = 'PFS req_cnt (> SU=' + str(SU) + ') ' + str(reqs) + ' ' + str(self.r)
        print msg
        self.logcollector.log(None, msg)

        # step 9:
        wins_p = []   # wins piece
        wins_l = []   # select peers
        while (len(wins_p) < SU):
            # 9.1
            (_rt, _p) = self.PFS_take_max() # (ratio, piece id)
            if _p not in wins_p:
                wins_p.append(_p)
            wins_l = self.PFS_select_leecher(_p, wins_l)
            # 9.2: update (decrease r_p)
            self.r[_p] -= 1.0
            # 9.3: update round
            self.round += 1
            # 9.4: update theta
            self.PFS_update_theta()

        # log output
        msg = 'PFS win pieces ' + str(wins_p)
        print msg
        self.logcollector.log(None, msg)
        
        # step 10, 11
        self.PFS_choke(wins_l)
        self.PFS_clear_structure()
        return True

    def PFS_clear_structure(self):
        for i, c in enumerate(self.connections):
            u = c.upload
            u.r.clear()
            u.I.clear()
        self.I.clear()
        self.r.clear()
        self.tm_first_req = 0

    def PFS_take_max(self):
        # find MAXi {r(i) / theta(i)}
        m_sets = []
        for (_p, _r) in self.r.iteritems():
            t = 0.00001
            if _p in self.theta:
                t += self.theta[_p]
            m_sets.append((_r/t, _p))
        #log output
        msg = 'PFS MAXi ' + str(max(m_sets)) + ': ' + str(m_sets)
        print msg
        self.logcollector.log(None, msg)
        return max(m_sets)
    
    def PFS_update_theta(self):
        if self.EPFS:
            #calculate theta_EPFS if EWMA mode
            alp = self.config['EPFS_alpha']
            for (_p, _t) in self.theta.iteritems():
                y = 0.0
                if _p in self.I:
                    y = 1.0
                self.theta[_p] = alp * y + (1.0 - alp) * self.theta[_p]
        else:   #PFS
            for (_p, _t) in self.I.iteritems():
                if _p in self.theta:
                    self.theta[_p] = (self.theta[_p] + 1) / self.round
                else:
                    self.theta[_p] = 1.0
    
    #step 10-11
    # find peers whose piece did not "win"
    def PFS_select_leecher(self, p, wins):
        for i, c in enumerate(self.connections):
            u = c.upload
            if not u.interested:
                continue
            if p in u.r:
                if c not in wins:
                    wins.append(c)
        return wins
    
    #step 10-11
    def PFS_choke(self, wins):
        # log output
        ipl = []
        for _c in wins:
            ipl.append(str(_c.ip))
        msg = 'PFS selected leechers ' + str(ipl)
        print msg
        self.logcollector.log(None, msg)

        ipl = []
        for i, c in enumerate(self.connections):
            u = c.upload
            if c not in wins:
                u.choke()
                ipl.append(str(c.ip))

        msg = 'PFS choke ' + str(ipl)
        print msg
        self.logcollector.log(None, msg)

    #EPFS end

    #unchoke and optimisitic unchoke when the local peer is a leecher
    def _rechoke(self):
        #Test if the peer is a seed. When in seed state (i.e., self.done()==True),
        #_rechoke is called by connection_lost(), interested(), and not_interested()
        if self.done():
            self._rechoke_seed()
            return
        
        #sort the peers (interested and not snubbed) by upload rate
        #in order to choose the four best uploads
        #to unchoke. The result of the sort is in the preferred[] list,
        #the highest upload rate first in the list.
        preferred = []
        #only for logging
        preferred_ip = []
        for i in xrange(len(self.connections)):
            c = self.connections[i]
            if c.upload.interested and not c.download.is_snubbed():
                preferred.append((-c.download.get_rate(), i))
                preferred_ip.append((-c.download.get_rate(), c.ip))
            if c.download.is_snubbed():
                self.logcollector.log(None,'SB ' + str(c.ip))
        preferred.sort()
        preferred_ip.sort()        

        self.logcollector.log('preferred(sorted) in _rechoke=: ' + str(preferred_ip))
        
        #prefcount is the number of unchoke to perform (max_uploads_internal=4 by default)
        #mask[i]=1 when connection[i] is to be unchoked
        #max_uploads_internal is automatically set in download.py depending on the
        #upload capacity. With the default setting max_uploads_internal=4.
        #SLICING: let A=(1,2,3,4) then A[a,b] is inclusive in a, but exclusive in b
        #A[2:]=(3,4)  A[:2]=(1,2)
        #When len(preferred)>2, prefcount=3
        prefcount = min(len(preferred), self.config['max_uploads_internal'] -1)
        mask = [0] * len(self.connections)
        for _, i in preferred[:prefcount]:
            mask[i] = 1
        
        #min_uploads: the number of uploads to fill out to with extra optimistic unchokes 
        #it is set to 2 by default.
        #Count is the number of optimistic unchoke to perform. We note that when a lot of connections
        #are snubbed, i.e., prefcount is 0, then count is equal to 2 for min_uploads=2. count is equal to 1
        #when prefcount>0.
        #WARNING: count and self.count are not related at all.
        count = max(1, self.config['min_uploads'] - prefcount)
        self.logcollector.log(None, 'INT LP ' + str(len(preferred)) +\
                              ' PC ' + str(prefcount) + ' C ' + str(count) + ' R ' + str(self.count))

        for i in xrange(len(self.connections)):
            c = self.connections[i]
            u = c.upload
            #regular unchoke
            if mask[i]:
                self.logcollector.log(None, 'PRU ' + str(c.ip))
                u.unchoke(self.count)

            #In case the peer to optimistically unchoke is not interested, it is unchoked, but it does
            #not count as optimistically unchoked, i.e., count is not decremented.
            #As a result, several NOT_INTERESTED peers (much more than 4) may be
            #unchoked, but only 4 are interested.
            #For all the not_interested ones and unchoked ones, as soon as the peer receive a INTERESTED
            #message, the _rechoke() method is called immediatly, without the need to wait for a 10 seconds period,
            #in order to keep only 4 unchoked and interested peers at a time.
            #The reason for unchoking NOT_INTERESTED peers could be that with this strategy,
            #you allow peers that send an interested message to immediatly trigger the _rechoke() method.
            #In this case there is not need to wait for 10 seconds. As when the peers to optimistically
            #unchoke are NOT_INTERESTED, you may end up with too few peers that uploads from you, this
            #policy would make sense to speed up the _rechoke().
            #
            #In the case of only one peer to optimistically unchoke,
            #it will be put fist in the connections[] list if it is interested and choked.
            #Thus it always count as optimistically unchoked, unless it is one of the four
            #best peers. In this case it will be chosen as regular unchoke in the test just before.
            #We have no guarantee that the next optimistic unchoke will be interested.
            #count it typically higher than one in the startup phase when no peer or few peers are interested,
            #as in the startup we do not have pieces or few pieces.
            #
            #Every 30 seconds a different peer to optimistically unchoke is chosen, but the _rechoke()
            #method is run every 10 seconds. However, as the first peer in the connection[]
            #list will not change, it will always be unchoked if it is interested during the
            #30 seconds period. In two cases a new peer will be chosen in the connections[] list within 10 seconds:
            #    -the regular (i.e. the one put first in the connections list) optimistic unchoke is no more interested
            #    -the regular optimistic unchoke becomes one of the three best preferred peers
            #In summary, there are always 4 peers unchoked (including the optimistic unchoke). If the regular
            #optimisitic unchoke never become one of the 3 best peers, it will be changed after 30 seconds. In case,
            #it becomes one of the 3 best peers, a new peer to optimistically unchoke will be heuristically chosen
            #(i.e. not chosen as the first peer in the connections[] list)
            #after 10 seconds. In any case, every 30 seconds a new choked and interested peer
            #is set first in the connections[] list.
            elif count > 0:
                if u.interested:
                    self.logcollector.log(None, 'POU I ' + str(c.ip))
                    count -= 1
                else:
                    self.logcollector.log(None, 'POU NI ' + str(c.ip))
                u.unchoke(self.count)
            else:
                u.choke()

    #optimisitic unchoke when the peer is in seed state
    #This method is called with force_new_unchokes==True by _round_robin every 10 seconds
    #This method is also called with force_new_unchokes==False by _rechoke each time
    #Choker.connection_lost(), Choker.interested(), and Choker.not_interested() is called.
    #variable definition:
    #i                  : number of unchokes to perform over a 30 seconds period
    #num_force_unchokes : number of unchokes to perform on a 10 seconds interval
    #num_kept           : number of connections to keep
    #num_nonpref        : number of connections that will be choked
    #unchokes_since_last: number of unchokes since the last regular call to _rechoke_seed
    def _rechoke_seed(self, force_new_unchokes = False):
        if force_new_unchokes:
            #As _rechoke_seed is called every 10 seconds, the number i of unchokes
            #to performs is spread out on the three 10 seconds periods.
            #For max_uploads_internal==4, i==2. 
            i = (self.config['max_uploads_internal'] + 2) // 3
            
            #num_force_unchokes is the number of unchokes to perform on this 10 seconds
            #interval. It takes into account i (so that three consecutive num_force_unchokes do
            #not exceed i) and unchokes_since_last, which is the number of unchokes performed
            #outside the 10 seconds interval, due to a connection_lost.
            num_force_unchokes = max(0, (i + self.count % 3) // 3 - \
                                 self.unchokes_since_last)
            self.logcollector.log(None, 'INT i ' + str(i))
        else:
            num_force_unchokes = 0

        preferred = []
        #only for logging
        preferred_ip = []
        new_limit = self.count - 3
        #For all the connections order first the connections unchoked recently
        #(unchoke_time>new_limit, i.e., in the last two rounds) or that have data to send
        #(u.buffer is not empty), most recent connections first then fastest
        #upload rate first; then the other connections, fastest upload rate first. 
        for i in xrange(len(self.connections)):
            c = self.connections[i]
            u = c.upload
            #lists and tuples are compared by comparing each component, from left to right
            #For instance (1,5)<(2,1)
            if not u.choked and u.interested:
                #u.buffer==[] if the uploader u does not have any pending request from
                #its remote peer, i.e., it we cannot send data immediatly to the peer if we
                #want so.
                #c.connection.is_flushed() is True is there is no data waiting for the
                #socket availability. If it is False, there might be a connection problem.
                if u.unchoke_time > new_limit or (
                        u.buffer and c.connection.is_flushed()):
                    preferred.append((-u.unchoke_time, -u.get_rate(), i))
                    preferred_ip.append((-u.unchoke_time, -u.get_rate(), c.ip))
                else:
                    preferred.append((1, -u.get_rate(), i))
                    preferred_ip.append((1, -u.get_rate(), c.ip))                    
        #num_kept is the number of connections to keep. The other connections can be choked
        #to free connections to perform new unchokes.
        num_kept = self.config['max_uploads_internal'] - num_force_unchokes
        assert num_kept >= 0
        preferred.sort()
        preferred_ip.sort()        
        self.logcollector.log('preferred(sorted) in _rechoke_seed=' + str(preferred_ip))
        preferred = preferred[:num_kept]
        mask = [0] * len(self.connections)
        for _, _, i in preferred:
            mask[i] = 1
        #num_nonpref is the number of connections that will be choked
        num_nonpref = self.config['max_uploads_internal'] - len(preferred)

        #unchokes_since_last is the number of unchokes since the last regular call to _rechoke_seed, i.e.,
        #the call that occurs every 10 seconds, not the accidental one triggered by a connection close.
        #As soon as a regular call to _rechoke_seed is performed, i.e., force_new_unchokes==True,
        #unchokes_since_last is reset to 0
        if force_new_unchokes:
            self.unchokes_since_last = 0
        else:
            self.unchokes_since_last += num_nonpref
        self.logcollector.log(None, 'INT NFU '+ str(num_force_unchokes) + ' NL ' + str(new_limit) +\
                              ' NK ' + str(num_kept) + ' NN ' + str(num_nonpref) + ' USL ' +\
                              str(self.unchokes_since_last) + ' R ' + str(self.count))

        last_unchoked = None
        for i in xrange(len(self.connections)):
            c = self.connections[i]
            u = c.upload
            #if not in the set of connection to keep
            if not mask[i]:
                #choke if NOT_INTERESTED
                if not u.interested:
                    u.choke()
                #if INTERESTED and CHOKED
                elif u.choked:
                    if num_nonpref > 0 and c.connection.is_flushed():
                        self.logcollector.log(None, 'SRU ' + str(c.ip))
                        u.unchoke(self.count)
                        num_nonpref -= 1
                        if num_nonpref == 0:
                            last_unchoked = i
                #if INTERESTED and UNCHOKED
                else:
                    if num_nonpref == 0:
                        u.choke()
                    else:
                        num_nonpref -= 1
                        if num_nonpref == 0:
                            last_unchoked = i
            #only for logging
            else:
                self.logcollector.log(None, 'SKU ' + str(c.ip))
            
        #shuffle the connection list cutting it after the last_unchoked
        #and switching both parts.
        if last_unchoked is not None:
            self.connections = self.connections[last_unchoked + 1:] + \
                               self.connections[:last_unchoked + 1]

    #new connections are inserted at a random place in the list connections[]
    #That is here that is build the random list of connections, thus the random
    #choice of the optimistic unchoke among the choked and interested connections.
    #This method is called by Encoder.py(connection_completed()). I log the connection
    #completed event (CONH) in Encoder.py
    def connection_made(self, connection):
        p = randrange(len(self.connections) + 1)
        self.connections.insert(p, connection)

    #this method is called by Connecter.py _sever(). I log the event of a connection
    #completed and closed (CONH C) in Connecter.py
    def connection_lost(self, connection):
        self.connections.remove(connection)
        if connection.upload.interested and not connection.upload.choked:
            self._rechoke()

    def interested(self, connection):
        if not connection.upload.choked:
            self._rechoke()

    def not_interested(self, connection):
        if not connection.upload.choked:
            self._rechoke()
