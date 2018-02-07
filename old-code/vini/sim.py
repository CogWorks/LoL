#!/usr/bin/env python

import sys
import os
import sqlite3
import scipy
import numpy

import argparse, gzip, struct, math

from scipy.spatial.distance import pdist

features = [
            "assists",
            "champLevel",
            "deaths",
            "doubleKills",
            "firstBloodAssist",
            "firstBloodKill",
            "firstInhibitorAssist",
            "firstInhibitorKill",
            "firstTowerAssist",
            "firstTowerKill",
            "goldEarned",
            "goldSpent",
            "inhibitorKills",
            "killingSprees",
            "kills",
            "largestCriticalStrike",
            "largestKillingSpree",
            "largestMultiKill",
            "magicDamageDealt",
            "magicDamageDealtToChampions",
            "magicDamageTaken",
            "minionsKilled",
            "neutralMinionsKilled",
            "neutralMinionsKilledEnemyJungle",
            "neutralMinionsKilledTeamJungle",
            "pentaKills",
            "physicalDamageDealt",
            "physicalDamageDealtToChampions",
            "physicalDamageTaken",
            "quadraKills",
            "sightWardsBoughtInGame",
            "totalDamageDealt",
            "totalDamageDealtToChampions",
            "totalDamageTaken",
            "totalHeal",
            "totalTimeCrowdControlDealt",
            "totalUnitsHealed",
            "towerKills",
            "tripleKills",
            "trueDamageDealt",
            "trueDamageDealtToChampions",
            "trueDamageTaken",
            "unrealKills",
            "visionWardsBoughtInGame",
            "wardsKilled",
            "wardsPlaced",
            "winner"
            ]

features_sql = "SELECT %s FROM matchHistory" % (",".join(features))

def nCr(n,r):
    f = math.factorial
    return f(n) / f(r) / f(n-r)

def get_lolFeatures(input_file):
    db = sqlite3.connect(input_file)
    cursor = db.cursor()
    cursor.execute(features_sql)
    lolFeatures = scipy.array(cursor.fetchall())
    db.close()
    return lolFeatures
    
def get_lolDistances(lolFeatures):
    return pdist(lolFeatures[0:lolFeatures.shape[0],],"cosine")
    
def save_lolDistances(lolDistances, N, output_file, block_size):
    fs = open(output_file,'wb')
    l = len(lolDistances)
    num_iter = l / block_size
    rem = l % block_size
    fs.write(struct.pack("Q", N))
    for i in range(num_iter):
        idx = i * block_size
        s = "%sH" % block_size
        fs.write(struct.pack(s, *(lolDistances[idx : idx + block_size] * 65535).round().astype(numpy.uint16)))
    fs.write(struct.pack("%sH" % rem, *(lolDistances[idx + block_size : idx + block_size + rem] * 65535).round().astype(numpy.uint16)))
    fs.close()
    
def load_lolDistances(input_file):
    fs = open(input_file,'rb')
    n = struct.unpack('Q', fs.read(struct.calcsize('Q')))[0]
    l = nCr(n, 2)
    lolDistances = numpy.fromfile(fs, dtype='uint16', count=l) / 65535.0
    fs.close()
    return lolDistances
    
    
    
if __name__ == '__main__':
    parser = argparse.ArgumentParser( formatter_class = argparse.ArgumentDefaultsHelpFormatter )
    parser.add_argument( '-I', '--input', action = "store", dest = "input_file", type = str, required=True, help = 'input file' )
    parser.add_argument( '-O', '--output', action = "store", dest = "output_file", type = str, default=None, help = 'output file' )
    parser.add_argument( '-B', '--blocks', action = "store", dest = "block_size",type = int, default = 1048576, help = 'read/write block size' )
    
    args = parser.parse_args()
    
    kind = os.path.splitext(args.input_file)[1]
    
    if kind == '.sqlite' or kind == '.sqlite3' or kind == '.db3' or kind == '.s3db':
        lolFeatures = get_lolFeatures(os.path.abspath(args.input_file))
        print(lolFeatures, lolFeatures.shape)
        lolDistances = get_lolDistances(lolFeatures)
        print(lolDistances, lolDistances.shape)
        if args.output_file != None:
            save_lolDistances(lolDistances, lolFeatures.shape[0], os.path.abspath(args.output_file), args.block_size)
    else:
        lolDistances = load_lolDistances(os.path.abspath(args.input_file))
        print(lolDistances, lolDistances.shape)
