#!/usr/bin/env python

import sys
import os
import sqlite3
import scipy
from sklearn.cluster import AffinityPropagation

features = [
            "assists",
            "champLevel",
            #"combatPlayerScore",
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
            #"item0",
            #"item1",
            #"item2",
            #"item3",
            #"item4",
            #"item5",
            #"item6",
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
            #"nodeCapture",
            #"nodeCaptureAssist",
            #"nodeNeutralize",
            #"nodeNeutralizeAssist",
            #"objectivePlayerScore",
            "pentaKills",
            "physicalDamageDealt",
            "physicalDamageDealtToChampions",
            "physicalDamageTaken",
            "quadraKills",
            "sightWardsBoughtInGame",
            #"teamObjective",
            "totalDamageDealt",
            "totalDamageDealtToChampions",
            "totalDamageTaken",
            "totalHeal",
            #"totalPlayerScore",
            #"totalScoreRank",
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

if __name__ == '__main__':
    
    datadir = os.path.join(os.path.dirname(os.path.realpath(__file__)),"data")
    db = sqlite3.connect(os.path.join(datadir,'matchHistory.sqlite3'))
    cursor = db.cursor()
    cursor.execute(features_sql)
    lolFeatures = scipy.array(cursor.fetchall())
    print(lolFeatures)
    af = AffinityPropagation(preference=-50).fit(lolFeatures)
    print(af)