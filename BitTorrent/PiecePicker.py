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

from random import randrange, shuffle, choice

#One object for all the connections.
#object that contains for each piece its status (have/not have),
#and the number of peer than have this piece
#Also implement the rarest first policy with the next() method
class PiecePicker(object):

    def __init__(self, numpieces, config):
        self.config = config
        self.numpieces = numpieces

        #interests in a list that contains at the indice i the list of the
        #pieces that are copied i times in the peer set.
        #it is initialized with all pieces copied zero time.
        self.interests = [range(numpieces)]
        self.pos_in_interests = range(numpieces)
        
        #numinterests[i] is the number of peers that have piece i
        self.numinterests = [0] * numpieces
        
        #have[i]=True when the local peer has piece i
        self.have = [False] * numpieces

        #seems to be used only in DownloaderFeedback.py
        self.crosscount = [numpieces]
        
        #maintains the pieces that was requested but not yet fully received
        self.started = []
        self.seedstarted = []
        
        #number of different pieces the local peer has received.
        self.numgot = 0

        #A random list of pieces used in random mode
        self.scrambled = range(numpieces)
        shuffle(self.scrambled)

    def got_have(self, piece):
        #piece is the index of the piece contained in the received
        #HAVE message
        numint = self.numinterests[piece]
        self.crosscount[numint + self.have[piece]] -= 1
        self.numinterests[piece] += 1
        try:
            self.crosscount[numint + 1 + self.have[piece]] += 1
        except IndexError:
            self.crosscount.append(1)
        if self.have[piece]:
            return
        #if there was still no pieces with numint copies, then increase
        #the list interests to add a new list for the pieces with numint copies.
        if numint == len(self.interests) - 1:
            self.interests.append([])
        self._shift_over(piece, self.interests[numint], self.interests[numint + 1])

    def lost_have(self, piece):
        numint = self.numinterests[piece]
        self.crosscount[numint + self.have[piece]] -= 1
        self.numinterests[piece] -= 1
        self.crosscount[numint - 1 + self.have[piece]] += 1
        if self.have[piece]:
            return
        self._shift_over(piece, self.interests[numint], self.interests[numint - 1])

    def _shift_over(self, piece, l1, l2):
        p = self.pos_in_interests[piece]
        #l1[-1] is the last piece in the list
        l1[p] = l1[-1]
        self.pos_in_interests[l1[-1]] = p
        del l1[-1]
        #insert the new piece in a random position
        newp = randrange(len(l2) + 1)
        if newp == len(l2):
            self.pos_in_interests[piece] = len(l2)
            l2.append(piece)
        else:
            old = l2[newp]
            self.pos_in_interests[old] = len(l2)
            l2.append(old)
            l2[newp] = piece
            self.pos_in_interests[piece] = newp

    def requested(self, piece, seed = False):
        if piece not in self.started:
            self.started.append(piece)
        if seed and piece not in self.seedstarted:
            self.seedstarted.append(piece)

    #called when the last block of a piece was just received
    def complete(self, piece):
        assert not self.have[piece]
        self.have[piece] = True
        self.crosscount[self.numinterests[piece]] -= 1
        try:
            self.crosscount[self.numinterests[piece] + 1] += 1
        except IndexError:
            self.crosscount.append(1)
        self.numgot += 1
        l = self.interests[self.numinterests[piece]]
        p = self.pos_in_interests[piece]
        l[p] = l[-1]
        self.pos_in_interests[l[-1]] = p
        del l[-1]
        try:
            self.started.remove(piece)
            self.seedstarted.remove(piece)
        except ValueError:
            pass

    def next(self, havefunc, seed = False):
        bests = None
        bestnum = 2 ** 30
        if seed:
            s = self.seedstarted
        else:
            s = self.started
        #return first the pieces already requested 
        for i in s:
            if havefunc(i):
                #if the piece already requested has more than bestnum copy,
                #do not return it. Returns the pieces already requested,
                #which are the less replicated in the peer set
                if self.numinterests[i] < bestnum:
                    bests = [i]
                    bestnum = self.numinterests[i]
                elif self.numinterests[i] == bestnum:
                    bests.append(i)
        if bests:
            return choice(bests)
        #if less than 3 pieces downloaded, random policy
        if self.numgot < self.config['rarest_first_cutoff']:
            for i in self.scrambled:
                if havefunc(i):
                    return i
            return None
        #rarest first policy
        for i in xrange(1, min(bestnum, len(self.interests))):
            for j in self.interests[i]:
                if havefunc(j):
                    return j
        return None

    def am_I_complete(self):
        return self.numgot == self.numpieces

    def bump(self, piece):
        l = self.interests[self.numinterests[piece]]
        pos = self.pos_in_interests[piece]
        del l[pos]
        l.append(piece)
        for i in range(pos,len(l)):
            self.pos_in_interests[l[i]] = i
        try:
            self.started.remove(piece)
            self.seedstarted.remove(piece)
        except ValueError:
            pass
