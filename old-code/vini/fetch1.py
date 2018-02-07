#!/usr/bin/env python

import sys
import os
import time
import sqlite3
from riotwatcher import RiotWatcher, NORTH_AMERICA
from riotwatcher.riotwatcher import LoLException

def merge_dicts(*dict_args):
    '''
    Given any number of dicts, shallow copy and merge into a new dict,
    precedence goes to key value pairs in latter dicts.
    '''
    result = {}
    for dictionary in dict_args:
        result.update(dictionary)
    return result

MATCH_COMMON = {#[type,key]
    "mapId":["INTEGER",False],
    "matchCreation":["INTEGER",False],
    "matchDuration":["INTEGER",False],
    "matchId":["INTEGER",True],
    "matchMode":["TEXT",False],
    "matchType":["TEXT",False],
    "matchVersion":["TEXT",False],
    "platformId":["TEXT",False],
    "queueType":["TEXT",False],
    "region":["TEXT",False],
    "season":["TEXT",False]
}

MATCH_PARTICIPANT = {
    "championId":["INTEGER",False],
    "highestAchievedSeasonTier":["STRING",False],
    "masteries":[None,False],
    "participantId":[None,False],
    "runes":[None,False],
    "spell1Id":["INTEGER",False],
    "spell2Id":["INTEGER",False],
    "stats":[None,False],
    "teamId":[None,False],
    "timeline":[None,False],
    "summonerName":["TEXT",False],
    "summonerId":["INTEGER",True]
}

MATCH_PARTICIPANT_STATS = {
    "assists":["INTEGER",False],
    "champLevel":["INTEGER",False],
    "combatPlayerScore":["INTEGER",False],
    "deaths":["INTEGER",False],
    "doubleKills":["INTEGER",False],
    "firstBloodAssist":["INTEGER",False],
    "firstBloodKill":["INTEGER",False],
    "firstInhibitorAssist":["INTEGER",False],
    "firstInhibitorKill":["INTEGER",False],
    "firstTowerAssist":["INTEGER",False],
    "firstTowerKill":["INTEGER",False],
    "goldEarned":["INTEGER",False],
    "goldSpent":["INTEGER",False],
    "inhibitorKills":["INTEGER",False],
    "item0":["INTEGER",False],
    "item1":["INTEGER",False],
    "item2":["INTEGER",False],
    "item3":["INTEGER",False],
    "item4":["INTEGER",False],
    "item5":["INTEGER",False],
    "item6":["INTEGER",False],
    "killingSprees":["INTEGER",False],
    "kills":["INTEGER",False],
    "largestCriticalStrike":["INTEGER",False],
    "largestKillingSpree":["INTEGER",False],
    "largestMultiKill":["INTEGER",False],
    "magicDamageDealt":["INTEGER",False],
    "magicDamageDealtToChampions":["INTEGER",False],
    "magicDamageTaken":["INTEGER",False],
    "minionsKilled":["INTEGER",False],
    "neutralMinionsKilled":["INTEGER",False],
    "neutralMinionsKilledEnemyJungle":["INTEGER",False],
    "neutralMinionsKilledTeamJungle":["INTEGER",False],
    "nodeCapture":["INTEGER",False],
    "nodeCaptureAssist":["INTEGER",False],
    "nodeNeutralize":["INTEGER",False],
    "nodeNeutralizeAssist":["INTEGER",False],
    "objectivePlayerScore":["INTEGER",False],
    "pentaKills":["INTEGER",False],
    "physicalDamageDealt":["INTEGER",False],
    "physicalDamageDealtToChampions":["INTEGER",False],
    "physicalDamageTaken":["INTEGER",False],
    "quadraKills":["INTEGER",False],
    "sightWardsBoughtInGame":["INTEGER",False],
    "teamObjective":["INTEGER",False],
    "totalDamageDealt":["INTEGER",False],
    "totalDamageDealtToChampions":["INTEGER",False],
    "totalDamageTaken":["INTEGER",False],
    "totalHeal":["INTEGER",False],
    "totalPlayerScore":["INTEGER",False],
    "totalScoreRank":["INTEGER",False],
    "totalTimeCrowdControlDealt":["INTEGER",False],
    "totalUnitsHealed":["INTEGER",False],
    "towerKills":["INTEGER",False],
    "tripleKills":["INTEGER",False],
    "trueDamageDealt":["INTEGER",False],
    "trueDamageDealtToChampions":["INTEGER",False],
    "trueDamageTaken":["INTEGER",False],
    "unrealKills":["INTEGER",False],
    "visionWardsBoughtInGame":["INTEGER",False],
    "wardsKilled":["INTEGER",False],
    "wardsPlaced":["INTEGER",False],
    "winner":["INTEGER",False]
}

TABLE_BASE_TEMPLATE = "CREATE TABLE matchHistory(_id INTEGER PRIMARY KEY, %s, unique(%s))"
TABLE_INSERT_TEMPLATE = "INSERT INTO matchHistory(%s) VALUES(%s)"

def build_sqlite3_columns(*args):
    mapping = merge_dicts(*args)
    return ", ".join(["%s %s" % (k,mapping[k][0]) for k in mapping if mapping[k][0]!=None])
    
def build_sqlite3_keys(*args, **kwargs):
    mapping = merge_dicts(*args)
    return ", ".join(["%s%s" % (kwargs.get('prefix',''),k) for k in mapping if mapping[k][0]!=None])
    
def build_sqlite3_unique(*args):
    mapping = merge_dicts(*args)
    return ", ".join(["%s" % k for k in mapping if mapping[k][1]==True])
    
def build_sqlite3_values(*args):
    mapping = merge_dicts(*args)
    return ",".join(["?"]*len([k for k in mapping if mapping[k][0]!=None]))

def build_insert_call(*args):
    mapping = merge_dicts(*args)
    return TABLE_INSERT_TEMPLATE % (build_sqlite3_keys(mapping),build_sqlite3_keys(mapping,prefix=":"))

def get_common_match_data(obj):
    return {k:obj[k] for k in MATCH_COMMON if MATCH_COMMON[k][0]!=None}
    
def get_participant_match_data(obj):
    return {k:obj[k] for k in MATCH_PARTICIPANT if MATCH_PARTICIPANT[k][0]!=None}
    
def get_participant_match_stat_data(obj):
    return {k:obj[k] if k in obj else None for k in MATCH_PARTICIPANT_STATS if MATCH_PARTICIPANT_STATS[k][0]!=None}

if __name__ == '__main__':

    # Check to make sure the data directory exists before creating the any files
    datadir = os.path.join(os.path.dirname(os.path.realpath(__file__)),"data")
    if not os.path.exists(datadir):
        os.makedirs(datadir)
    
    # Establish a connection with the database
    db = sqlite3.connect(os.path.join(datadir,'matchHistory.sqlite3'))
    cursor = db.cursor()
    
    # Check to see if the matchHistory table already exists in the database
    lastSummonerId = None
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='matchHistory'")
    if cursor.fetchone()==None:
        # If the matchHistory table doesn't exists, create it
        cursor.execute(TABLE_BASE_TEMPLATE % (build_sqlite3_columns(MATCH_COMMON,MATCH_PARTICIPANT,MATCH_PARTICIPANT_STATS),
                                          build_sqlite3_unique(MATCH_COMMON,MATCH_PARTICIPANT,MATCH_PARTICIPANT_STATS)))
    else:
        # Find last summonerId
        cursor.execute("SELECT summonerId FROM matchHistory WHERE _id=(SELECT max(_id) FROM matchHistory)")
        lastSummonerId = cursor.fetchone()[0]
        #delete = "DELETE FROM matchHistory WHERE summonerId=%d" % lastSummonerId
        #cursor.execute(delete)
        #print(cursor.fetchone())
    
    # Load a riot developer key as a string
    with open (os.path.expanduser("~/.riot.key"), "r") as keyfile:
        w = RiotWatcher(keyfile.read().splitlines()[0])
        # Load the list of NA summoner Ids
        with open (os.path.join(os.path.dirname(os.path.realpath(__file__)), 'summoner_ids.txt'), "r") as sidfile:
            sids = [int(sid) for sid in sidfile.read().splitlines()]
            # Loop though list of summoners and get their data
            for sid in sids:
                if lastSummonerId != None:
                    if sid == lastSummonerId:
                        lastSummonerId = None
                    else:
                        continue
                idx = 0
                inc = 1
                retry = 0
                while True:
                    try:
                        # Check to make sure we can make a request so that we don't exceede the rate limit
                        while not w.can_make_request():
                            time.sleep(0)
                        matches = w.get_match_history(sid,region=NORTH_AMERICA,ranked_queues=['RANKED_SOLO_5x5'],begin_index=idx,end_index=idx+inc)
                        if "matches" in matches:
                            for m in matches["matches"]:
                                print((sid,m["matchId"])),
                                match = get_common_match_data(m)
                                participants = {}
                                for p in m["participants"]:
                                    participants[p["participantId"]] = p
                                for p in m["participantIdentities"]:
                                    participants[p["participantId"]].update(p["player"])
                                for p in participants:
                                    if participants[p]['summonerId']==sid:
                                        participant = get_participant_match_data(participants[p])
                                        stats = get_participant_match_stat_data(participants[p]['stats'])
                                        d = merge_dicts(match,participant,stats)
                                        c = build_insert_call(MATCH_COMMON,MATCH_PARTICIPANT,MATCH_PARTICIPANT_STATS)
                                        try:
                                            cursor.execute(c, d)
                                            print("")
                                        except sqlite3.IntegrityError as e:
                                            print(" ~ skipping duplicate")
                                            
                            idx += inc
                        else:
                            break
                        db.commit()
                    except LoLException as e:
                        print("[LoLException] %s" % e)
                        if retry < 5:
                            retry += 1
                            print("Retry %d of 5" % retry)
                        else:
                            break
    db.close()