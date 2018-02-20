#!/usr/bin/python

## MUST INSTALL:
## SSHTUNNEL
## RIOTWATCHER
## mysql-connector-python

import ssl
import riotwatcher
import urllib2
import json
import requests
import time
import csv
import curses
from twisted.internet import task
from twisted.internet import reactor
from os import path
import os
from requests import HTTPError

from utils import todict
import mysql.connector
from mysql.connector import errorcode


from time import sleep

import credentials

##making ssh option a command line option
import sys, getopt
import argparse
if __name__ == '__main__' :

 parser = argparse.ArgumentParser(description="LoL Scraper", formatter_class=argparse.ArgumentDefaultsHelpFormatter)
 parser.add_argument("-s", dest="ssh", required=False, help="using ssh or not", metavar="ssh")
 args = parser.parse_args()
 ssh = args.ssh
 
 

def strip_to_list (data_raw):
 data = []
 for x in data_raw:
  for y in x:
   data.append(y)
 return data

class Scraper:
 
 
 def __init__(self, ssh=True, use_curses=False, rate_limiting=True, rate = "Slow", skipfile=False):
  self.rate_limiting = rate_limiting
  self.rate = rate
  if ssh == "True":
   ssh = True
  elif ssh == "False":
   ssh = False
  elif ssh != False and ssh != True:
   print "Not a valid value for ssh, defaulting to True."
   ssh = True
  if use_curses == "True":
   self.use_curses = True
  elif use_curses == "False":
   self.use_curses = False
  elif use_curses != False and use_curses != True:
   print "Not a valid value for use_curses, defaulting to False."
   self.use_curses = False
  elif use_curses == False or use_curses == True:
   self.use_curses = use_curses
  
# using SSH Tunnel because to connect directly to MySQL on server we need to comment out 
# 'skip-networking' in /etc/mysql/my.cnf which allows non-local connections and is generally less secure
# and additionally we'd have to change bind-address from 127.0.0.1 to 0.0.0.0

# server = credentials.server
# server.start()


##only use this if you know what you're doing.
  requests.packages.urllib3.disable_warnings()

  try:
   self.cnx = mysql.connector.connect(**credentials.config(ssh=ssh))
  except mysql.connector.Error as err:
    if err.errno == errorcode.ER_ACCESS_DENIED_ERROR:
      print("Something is wrong with your user name or password")
    elif err.errno == errorcode.ER_BAD_DB_ERROR:
      print("Database does not exist")
    else:
      print(err)
  else:
    print "Connected to %s database" % credentials.config()['database']
  self.cursor = self.cnx.cursor()
 
  keys_temp = credentials.keys
  

  self.keydict = {x:[0,0] for x in keys_temp}
  self.key = self.keydict.keys()[0]
  if skipfile:
    self.skipfiler = open(path.join(skipfile,'skiplist.tsv'),"rb+")
  else:
    self.skipfiler = open(path.join(os.getcwd(), 'skiplist.tsv'), "rb+")
  self.skiplist = self.skipfiler.read()
  self.skipfiler.close()
  self.old_count = 0
  if self.use_curses == True:
   self.stdscr = curses.initscr()
   self.stdscr.addstr(0, 0, "League of Legends Scraper", curses.A_BOLD)
   self.stdscr.refresh()
   self.msgs = []
   self.errormsg = []
  
  self.checkcount = 0
  self.timeout = 10 # Ten seconds
  self.initialize_reactor


 def initialize_reactor(self):
  self.l = task.LoopingCall(self.check_keys)
  self.l.start(self.timeout) # call every ten seconds

  self.reactor.run()



 def check_keys(self):
  self.checkcount += 1
  if self.checkcount == 60:
   tenmin = True
   self.checkcount = 0 
  else:
   tenmin = False
  if self.rate_limiting == True:
   if tenmin == True:
    self.keydict = {x:[0,0] for x in self.keydict.keys()}
   else:
    for k in self.keydict.keys():
     self.keydict[k][0] = 0
    


 def wait(self):
  if self.rate_limiting == True:
   while not self.w.can_make_request():
    time.sleep(0.1)
   if self.rate == "Slow":
    rate1 = 10
    rate2 = 500
   else:
    rate1 = 3000
    rate2 = 180000
   if self.keydict[self.key][0] >= rate1 or self.keydict[self.key][1] >= rate2:
    self.new_key()
    
    
 def write_to_skip(self, teams):
  with open(path.join(path.dirname(__file__), 'skiplist.tsv'), "ab+") as skipfilew:
   for x in teams:
    skipfilew.write("%s\n" % (x))
  skipfilew.close()
 
 def print_stuff(self, msg=None, header1 = False, header2 = False, clear = False, error = False, progress = False, override = False):
  feedback = self.feedback
  if msg is None:
    msg = " "
  if self.use_curses == True:
   if clear == True:
    self.stdscr.erase()
    self.stdscr.addstr(0, 0, "League of Legends Scraper", curses.A_BOLD)
   if feedback != "silent" and header1 == True:
    self.stdscr.erase()
    self.stdscr.addstr(0, 0, "League of Legends Scraper", curses.A_BOLD)
    self.stdscr.addstr(1, 1, msg) 
   elif (feedback != "silent" and header2 == True) or progress == True:
    if (progress == True and feedback == "all") or progress == False:
     self.stdscr.addstr(2, 1, msg)  
   elif feedback != "silent" and error == True:
    self.errormsg.append(msg)
    if len(self.errormsg) > 2:
     del self.errormsg[1]
    for x in self.errormsg:
     self.stdscr.addstr(9+self.msgs.index(x),2,x)
   if header1 == False and header2 == False and (feedback == "all" or (override == True and feedback != "silent")):
    self.msgs.append(msg)
    if len(self.msgs) > 5:
     del self.msgs[1]
    for x in self.msgs:
     self.stdscr.addstr(4+self.msgs.index(x),3,x)

   self.stdscr.refresh()
  elif self.use_curses == False:
   if feedback != "silent" and (header1 == True or header2 == True or error == True or override == True):
    print msg
   elif feedback == "all" and not (header1 == True or header2 == True or error == True or override == True):
    print msg

    
     
    
   

 
    

 def new_key (self, t=None, drop=False):
  global w
#   global key

  if t:
   self.key = t
 
  if self.key == self.keydict.keys()[len(self.keydict)-1]:
   if drop == True:
    del self.keydict[self.key]
   self.key = self.keydict.keys()[0]
  else:
   if len(self.keydict)>1:
    if drop == True:
     new_key = self.keydict.keys()[self.keydict.keys().index(self.key)+1] 
     del self.keydict[self.key]
     self.key = new_key
    else:
     self.key = self.keydict.keys()[self.keydict.keys().index(self.key)+1] 
   else:
    self.print_stuff("Only one key.")
    if drop == True:
     self.print_stuff("Can't drop only key. Breaking.")
     self.wait()
    self.key = self.keydict.keys()[0]
 
 
 
 #  if riotwatcher.RiotWatcher(key).can_make_request():

  if self.rate.lower() == "fast":
#   Not sure how to establish fast rate limit with new api, need to implement new solution?
#    self.w = riotwatcher.RiotWatcher(self.key, limits=(riotwatcher.RateLimit(1500,10), riotwatcher.RateLimit(90000,600)))

    self.w = riotwatcher.RiotWatcher(self.key)
    
  else:
   self.w = riotwatcher.RiotWatcher(self.key)
  self.wait()

 #  print self.w.can_make_request()
 #  else: 
 #   if rate == len(keys):
 #    time.sleep(3)
 #   else:
 #    print "Rate Limit Reached"
 #    rate += 1
 #    self.new_key(t = key, rate=rate)
 
 def get_membertiers(self, matchIds=None, summonerIds=None, old_count = 0, full_count=None):
    add_league = ("INSERT IGNORE INTO by_league "
                "(isFreshBlood, division, isVeteran, wins, losses, playerOrTeamId, playerOrTeamName, isInactive, isHotStreak, leaguePoints, league, team, queue) "
                "VALUES (%(isFreshBlood)s, %(division)s, %(isVeteran)s, %(wins)s, %(losses)s, %(playerOrTeamId)s, %(playerOrTeamName)s, %(isInactive)s, %(isHotStreak)s, %(leaguePoints)s, %(league)s, %(team)s, %(queue)s)")

    summoner_ids_raw = [] 
    if matchIds:
     self.print_stuff("Extracting participant ids from %s matches" % len(matchIds))

     self.cursor.execute('SELECT DISTINCT(summonerId) FROM match_participants where matchId in ({0})'.format(', '.join(str(x) for x in matchIds)))
     summoner_ids_raw = self.cursor.fetchall()

     self.print_stuff("Finished extracting participants.")
     summoner_ids = [unicode(x[0]) for x in summoner_ids_raw] 
    
    
  
#      for x in summoner_ids_raw:
#       for y in x:
#        summoner_ids.append(y)
     summoner_ids =  list(set(summoner_ids)-set(self.existing_entries))
   
    elif summonerIds:
     self.print_stuff("Using supplied list of summoners.")
     summoner_ids = summonerIds
     old_count = 0 
     summoner_ids =  list(set(summoner_ids)-set(self.existing_entries))
     full_count = len(summoner_ids)



    
    for x in xrange(0,(int(len(summoner_ids)/10)+1)):
     stop = ((x+1)*10)

     if x == int(len(summoner_ids)/10):
      stop = (len(summoner_ids))
     if stop == x*10:
      continue
    
 #     print summoner_ids[(x*10):stop]
    
     league_entries = self.get_leagues(ids=summoner_ids,x=x, stop=stop, key=self.key, unauthorized_cycle=False)

     by_leagues = []
     for z in league_entries:
      for y in league_entries[z]:
       if "entries" in y:
        for v in y['entries']:
         v['league'] = y['tier']
         v['team'] = (True if y['queue']!="RANKED_SOLO_5x5" else False)
         v['queue'] = y['queue']
  #  for right now we're just going to discard miniSeries data
         if "miniSeries" in v:
          del v['miniSeries']
         by_leagues.append(v)

    
     try:
      self.cursor.executemany(add_league, by_leagues)
     except mysql.connector.Error as err:
      if err.errno != 1062 or self.suppress_duplicates == False:
       self.print_stuff("%s - Member-Tiers" % err.msg, error = True)

     else:
      self.print_stuff("Updated Member-Tiers", override = True)
    
     self.cnx.commit()
     if stop==(len(summoner_ids)):
      self.print_stuff("Finished %s of %s" % (stop+old_count, full_count), header2 = True)
     else:
      self.print_stuff("Finished %s of %s" % (stop+1+old_count, full_count), progress = True)
     self.old_count = len(summoner_ids) + old_count
     self.cnx.commit() 
       
         
 def get_leagues(self, ids=None,x=None, stop=None, key=None, unauthorized_cycle=False):
  finished = False
  unauthorized_key = False
  while finished == False:
   err = []
   self.wait()
   try:
     league_entries = self.w.league.by_summoner(self._region, summoner_ids=ids[(x*10):stop]) if unauthorized_cycle==False else self.w.league.by_summoner(self._region, summoner_ids=ids)
   except HTTPError as err:
        if err.response.status_code == 429:
            print('We should retry in {} seconds.'.format(e.headers['Retry-After']))
#             print('this retry-after is handled by default by the RiotWatcher library')
#             print('future requests wait until the retry-after time passes')
        elif err.response.status_code == 404:
            print('League not found.')
        else:
            raise
# 
#    except riotwatcher.riotwatcher.LoLException as err:
#     drop = False
#     if str(err) == "Unauthorized" :
# 
#      
#      self.print_stuff("Unauthorized, using new key") 
#  # This to ensure that you only try one new key for unauthorized, just switch truth value
#    
#      unauthorized_key= (True if unauthorized_key == False else False)
#  #         print unauthorized_key
# 
#  #       print str(err)
#     if str(err) == "Too many requests" or str(err) == "Blacklisted key":
#      if str(err) == "Blacklisted key":
#       self.print_stuff("Blacklisted key, using new key, dropping current.")
#       drop = True
#      unauthorized_key=False
#      
#      if str(err) != "Unauthorized" and str(err) != "Too many requests" and str(err) != "Blacklisted key":
#       
#       self.print_stuff("Break, %s" % (str(err)), error = True)
#       break 
# 
# 
#    
#  # Checks to make sure the error is not just coming from missing data     
#     if str(err) == "Game data not found":
#      
#      self.print_stuff("No data, skipping %s" % (team_ids if unauthorized_cycle == True else team_ids[(x*10):stop]))
#      league_entries = {}
#      if unauthorized_cycle == True:
#       self.write_to_skip(team_ids)
#      else: 
#       self.write_to_skip(team_ids[(x*10):stop])
#      finished = True   
#  # Basically this checks to see if unauthorized_key has been used and switched back to false. This should only happen if you have two unauthorized key changes in a row
#     elif unauthorized_key == True or str(err) != "Unauthorized":
#      self.print_stuff("New key assigned, %s" % str(err))
#  #         print str(err)
#     
#      self.new_key(drop = drop)
#     elif unauthorized_key == False and str(err) == "Unauthorized":
#      league_entries = {}
#      if unauthorized_cycle== False:
#       self.print_stuff("Unauthorized, checking individual")
#       for s in team_ids[(x*10):stop]:
#        if team == False:
#         time.sleep(0.5)
#        unauthorized_key=False
#  #       print team
#        cur_entry = self.get_leagues(ids=[s], x=x, stop=stop, key=self.key, unauthorized_cycle=True)
#        print cur_entry 
#        if cur_entry != {}:
#         league_entries[s] = cur_entry[s]
#        else:
#  #            print "No entry for %s" % s
#         continue
# 
#        finished = True
#      else:         
#  #          print "skipping %s" % team_ids
#       league_entries = {}
#       finished = True
#    
# 
   else: 
 #        print "Success"
 #        print league_entries
    finished = True
 
  return league_entries
 
 
 
 
 def get_indhistory(self, summoner_id=None, season=None, end_time=None):
 
     add_history = ("INSERT IGNORE INTO individual_history"
             "(summonerId, championId, lane, matchId, platformId, queue, region, role, season, timestamp)" 
             "VALUES (%(summonerId)s, %(championId)s, %(lane)s, %(matchId)s, %(platformId)s, %(queue)s, %(region)s, %(role)s, %(season)s, %(timestamp)s)" )
     add_summoner = ("REPLACE INTO summoner_list"
             "(summonerId, constr)" 
             "VALUES (%(summonerId)s, %(constr)s)" )
     
     err = []
     finished = False
     while finished == False:
      self.wait()
      try:
       cur_matchlist = self.w.get_match_list(summoner_id, season=season ,end_time=end_time)
      except HTTPError as err:
        if err.response.status_code == 429:
            print('We should retry in {} seconds.'.format(e.headers['Retry-After']))
#             print('this retry-after is handled by default by the RiotWatcher library')
#             print('future requests wait until the retry-after time passes')
        elif err.response.status_code == 404:
            finished=True
            print('Summoner not found.')
        else:
            raise
 #      
#       except riotwatcher.riotwatcher.LoLException as err:
#  #       print str(err)
#        if str(err) == "Game data not found":
#         finished = True
#        elif (str(err) == "Too many requests" and 'Retry-After' in err.headers) or str(err) == "Unauthorized" or str(err) == "Blacklisted key":
# #         print "New Key" 
#         drop = False
#         if str(err) == "Blacklisted key":
#          self.print_stuff("Blacklisted key, using new key, dropping current.")
#          drop = True
#         if str(err) == "Unauthorized":
#          self.print_stuff("Unauthorized, using new key")
#         if len(self.keydict)==1:
#          self.print_stuff("Too many requests, not enough keys.", error=True)
#          if hangwait == False:
#           break 
#          else:
#           time.sleep(0.5)
#         self.new_key(drop=drop)
#        elif (str(err) == "Too many requests" and 'Retry-After' not in err.headers):
#         time.sleep(0.01)       
#        else:
# 
#         self.print_stuff("%s, Summoner: %s" % (str(err), summoner_id), error= True)
#         break
      else:
       finished = True

     if str(err) == "Game data not found":
       err = []
       self.print_stuff("Game data not found", error=True)
       return
     
     summoner_update = {}
     summoner_update['summonerId'] = summoner_id
     summoner_update['constr'] = end_time
#      print summoner_update
#      try:
     self.cursor.execute(add_summoner, summoner_update)     
     
     ind_history = True 
     all_history = []
     try:
      for n in cur_matchlist['matches']:
       cur_history = {}
       cur_history['summonerId'] = summoner_id
       cur_history['championId'] = n['champion']
       for z in ['lane', 'matchId', 'platformId', 'queue', 'region', 'role', 'season', 'timestamp']:
        cur_history[z] = n[z]
       all_history.append(cur_history)
     except:
      ind_history = False
      self.print_stuff("No Individual History")
     
     if ind_history == True:
      try:
       self.cursor.executemany(add_history, all_history)
 #       print test_team
      except mysql.connector.Error as err:
       if err.errno != 1062 or self.suppress_duplicates == False:
        self.print_stuff( "Error %s : %s" % (err.errno,summoner_id), error = True)
      else:
       self.print_stuff("Updated Individual History")
     


     self.cnx.commit()
 #       print test_team
    #  except mysql.connector.Error as err:
#       if err.errno != 1062 or self.suppress_duplicates == False:
#        self.print_stuff( "Error %s : %s -- Summoner List" % (err.errno,summoner_id), error = True)
#        
        
 def get_aggstats(self, summoner_id=None, season="ALL"):  
  add_stats = ("INSERT IGNORE INTO stats"
       "(summonerId, season, playerStatSummaryType, modifyDate, wins, losses)"
       "VALUES (%(summonerId)s, %(season)s, %(playerStatSummaryType)s, %(modifyDate)s, %(wins)s, %(losses)s)" )
   
  add_substats = ("INSERT IGNORE INTO stats_aggregate"
       "(summonerId, season, playerStatSummaryType, aggregatedStat, aggregatedStatValue)"
       "VALUES (%(summonerId)s, %(season)s, %(playerStatSummaryType)s, %(aggregatedStat)s, %(aggregatedStatValue)s)")
  
  if season == "ALL":
   seasons = ["3", "2014", "2015", "2016"]
  elif season == "3" or season == "2014" or season == "2015" or season == "2016":
   seasons = [season]
  elif isinstance(season, list):
   all_l = ["3", "2014", "2015", "2016"]
   seasons = list(set(all_l) - (set(all_l)-set(season)))
  else:
   self.print_stuff("Invalid Season, Defaulting to ALL")
   seasons = ["3", "2014", "2015", "2016"]
  
  err = []
  all_stats = []
  all_substats = []

  for x in seasons:
   finished = False
   while finished == False:
    self.wait()
    try:
     cur_stats_all = self.w.get_stat_summary(summoner_id, season=x)
    except HTTPError as err:
        if err.response.status_code == 429:
            print('We should retry in {} seconds.'.format(e.headers['Retry-After']))
#             print('this retry-after is handled by default by the RiotWatcher library')
#             print('future requests wait until the retry-after time passes')
        elif err.response.status_code == 404:
            print('Summoner not found.')
            finished=True
        else:
            raise    
#     except riotwatcher.riotwatcher.LoLException as err:
# #       print str(err)
#      if str(err) == "Game data not found":
#       finished = True
#      elif (str(err) == "Too many requests" and 'Retry-After' in err.headers) or str(err) == "Unauthorized" or str(err) == "Blacklisted key":
# #         print "New Key" 
#       drop = False
#       if str(err) == "Blacklisted key":
#        self.print_stuff("Blacklisted key, using new key, dropping current.")
#        drop = True
#       if str(err) == "Unauthorized":
#        self.print_stuff("Unauthorized, using new key")
#       if len(self.keydict)==1:
#        self.print_stuff("Too many requests, not enough keys.", error=True)
#        if hangwait == False:
#         break 
#        else:
#         time.sleep(0.5)
#       self.new_key(drop=drop)
#      elif (str(err) == "Too many requests" and 'Retry-After' not in err.headers):
#       time.sleep(0.01)
#      else:
# 
#       self.print_stuff("%s, Summoner: %s" % (str(err), summoner_id), error= True)
#       break
    else:
     finished = True
  

   if cur_stats_all:       
    for y in cur_stats_all["playerStatSummaries"]:
     cur_stats = {}
     cur_substats = {}
     cur_stats['summonerId'] = summoner_id
     cur_stats['season'] = x
     for z in ['playerStatSummaryType', 'modifyDate', 'wins', 'losses']:
      try:
       cur_stats[z] = y[z]
      except:
       cur_stats[z] = None
     all_stats.append(cur_stats)
     if y["aggregatedStats"]:
      if y["aggregatedStats"] != {}:
       for z in y["aggregatedStats"].keys():
        cur_substats = {}
        cur_substats['summonerId'] = summoner_id
        cur_substats['season'] = x
        cur_substats['playerStatSummaryType'] = cur_stats['playerStatSummaryType']
        cur_substats['aggregatedStat'] = z
        cur_substats['aggregatedStatValue'] = y["aggregatedStats"][z]
        all_substats.append(cur_substats)
     
        
  try:
   self.cursor.executemany(add_stats, all_stats)
 #       print test_team
  except mysql.connector.Error as err:
   if err.errno != 1062 or self.suppress_duplicates == False:
    self.print_stuff( "Error %s : %s" % (err.errno,summoner_id), error = True)
  else:
   self.print_stuff("Updated Stats")    
  try:
   self.cursor.executemany(add_substats, all_substats)
 #       print test_team
  except mysql.connector.Error as err:
   if err.errno != 1062 or self.suppress_duplicates == False:
    self.print_stuff( "Error %s : %s" % (err.errno,summoner_id), error = True)
  else:
   self.print_stuff("Updated Sub-Stats")    
  self.cnx.commit() 
        
  

  
  
   
     

 def update_table(self, table, region="na1", queue="RANKED_TEAM_5x5", iteratestart=1, iterate=100, teamIds=False, matchIds=False, summonerIds=False, checkTeams= False, hangwait=False, feedback="all", suppress_duplicates = False, timeline = False, allow_updates=False, ignore_skiplist=False, just_teams = True, timeline_update=False, season=None, end_time=None, SQL=True):
  self._region = region
  self.SQL = SQL
  self.mat = self.mte = self.mp = self.mteam = self.mt = self.mtb = self.mpr = self.mpm = self.mpd = self.mtea = 1
  feedback = feedback.lower()
  if feedback != "all" and feedback != "quiet" and feedback != "silent":
   self.feedback="all"
   self.print_stuff("Invalid value for 'Feedback' option, reverting to default. All feedback will be shown.")
   feedback = "all"
  if feedback == "quiet" or feedback == "silent":
   suppress_duplicates = True
  self.feedback = feedback
  self.key = self.keydict.keys()[0]
  self.new_key()
  self.suppress_duplicates = suppress_duplicates


  if table=="challenger":
   add_challenger = ("INSERT IGNORE INTO by_league "
                "(isFreshBlood, division, isVeteran, wins, losses, playerOrTeamId, playerOrTeamName, isInactive, isHotStreak, leaguePoints, league, team, queue) "
                "VALUES (%(isFreshBlood)s, %(division)s, %(isVeteran)s, %(wins)s, %(losses)s, %(playerOrTeamId)s, %(playerOrTeamName)s, %(isInactive)s, %(isHotStreak)s, %(leaguePoints)s, %(league)s, %(team)s, %(queue)s)")
   self.print_stuff("Checking Challenger tier League API", header1 = True)
   self.wait()
   challenger = self.w.league.challenger_by_queue(self._region, queue=queue)

   s = []
   for x in todict(challenger)['entries']:
    x['league'] = "challenger"
    x['team'] = (True if queue!="RANKED_SOLO_5x5" else False)
    x['queue'] = queue
    s.append(x)
   
   try:
    self.cursor.executemany(add_challenger, s)


   except mysql.connector.Error as err:
   ##error 1062 is duplicate entry error for mysql
    if err.errno == 1062:
     if self.suppress_duplicates == False :
      self.print_stuff("Error %s" % (err.errno), error=True)
    else:
     self.print_stuff("Error %s" % (err.errno), error=True)
         

    
   else:
    self.print_stuff("Updated Challenger", header2 = True)
  
   if err.errno == 1062:
    self.print_stuff("Finished Challenger", header2 = True)
   
  
  
  
  if table=="master":
   add_master = ("INSERT IGNORE INTO by_league "
                "(isFreshBlood, division, isVeteran, wins, losses, playerOrTeamId, playerOrTeamName, isInactive, isHotStreak, leaguePoints, league, team, queue) "
                "VALUES (%(isFreshBlood)s, %(division)s, %(isVeteran)s, %(wins)s, %(losses)s, %(playerOrTeamId)s, %(playerOrTeamName)s, %(isInactive)s, %(isHotStreak)s, %(leaguePoints)s, %(league)s, %(team)s, %(queue)s)")
   self.print_stuff("Checking Master tier League API", header1 = True)
   self.wait()
   master = self.w.league.masters_by_queue(self._region, queue=queue)


   s = []
   for x in todict(master)['entries']:
    x['league'] = "master"
    x['team'] = (True if queue!="RANKED_SOLO_5x5" else False)
    x['queue'] = queue
    s.append(x)
   
   try:
      self.cursor.executemany(add_master, s)
   
   except mysql.connector.Error as err:
   ##error 1062 is duplicate entry error for mysql
    if err.errno == 1062:
     if self.suppress_duplicates == False:
      self.print_stuff("Error %s" % (err.errno), error = True)
    else:
     self.print_stuff("Error %s" % (err.errno), error = True)
    
    
   else:

    self.print_stuff("Updated Master", header2 = True)

    
   if err.errno == 1062:
    self.print_stuff("Finished Master", header2 = True) 

  if table=="membertiers":
    self.print_stuff("Updating Member-tiers.", header1 = True)
    if matchIds == False:
       self.print_stuff("No matches given, searching all summoners from all matches.")
       self.cursor.execute("SELECT DISTINCT(summonerId) FROM match_participants")
       sum_id_raw = self.cursor.fetchall()
       summonerIds= [unicode(x[0]) for x in sum_id_raw]
       
#           old method
#      self.print_stuff("No matches given, using all.")
#      self.cursor.execute("SELECT matchId FROM matches")
#      matchIds= strip_to_list(self.cursor.fetchall())
#      
#      self.cursor.execute("SELECT count(summonerId) FROM match_participants")
#      fullcount=self.cursor.fetchall()
#      fullcount=fullcount[0][0]
    else:
     self.cursor.execute('SELECT count(summonerId) FROM match_participants where matchId in ({0})'.format(', '.join(str(x) for x in matchIds)))
     fullcount=self.cursor.fetchall()
     fullcount=fullcount[0][0]
    
    self.old_count = 0 
    
    self.existing_entries = []
    
    self.cursor.execute("SELECT DISTINCT(playerOrTeamId) from by_league where team = 0")
    existing_entries_raw = self.cursor.fetchall()

    
    if allow_updates == False:
     self.existing_entries = [unicode(x[0]) for x in existing_entries_raw]
#      for x in existing_entries_raw:
#       for y in x:
#        self.existing_entries.append(y)
    if ignore_skiplist == False:
     self.existing_entries.append(x for x in self.skiplist)

    
    if matchIds == False: 
     self.get_membertiers(summonerIds=summonerIds)
     
    else:
     while len(matchIds) > 250:
      self.get_membertiers(matchIds = matchIds[:250], old_count = self.old_count, full_count=fullcount)
      del matchIds[:250]

    
    
     self.get_membertiers(matchIds, old_count = self.old_count, full_count=fullcount)
  
  if table=="individualhistory":
   self.print_stuff("Updating Individual History", header1 = True)
   if summonerIds == False:         
    if just_teams == True:
     self.print_stuff("No IDs supplied, searching teams.")
     self.cursor.execute("SELECT DISTINCT(playerId) FROM team_roster")

    else:
     self.print_stuff("No IDs supplied, searching matches.")
     self.cursor.execute("SELECT DISTINCT(summonerId) FROM match_participants")
    summoner_ids_raw = self.cursor.fetchall()
    summoner_ids = [unicode(x[0]) for x in summoner_ids_raw] 
#     for x in summoner_ids_raw:
#      for y in x:
#       summoner_ids.append(y)
   else:
    self.print_stuff("IDs supplied.")

    summoner_ids = summonerIds   
   
   if allow_updates == False:
    self.cursor.execute("SELECT DISTINCT(summonerId) FROM summoner_list")
    existing_ids_raw = self.cursor.fetchall()
    existing_ids = [unicode(x[0]) for x in existing_ids_raw]
    summoner_ids = list(set(summoner_ids)-set(existing_ids))
    
   curcount = 0
   for x in summoner_ids:
     
    self.get_indhistory(x, season=season, end_time=end_time)
    if summoner_ids.index(x) % 10 == 0:
     self.print_stuff("Finished %s of %s" % (summoner_ids.index(x), len(summoner_ids)), progress=True)
    
    
   self.print_stuff("Finished %s of %s" % (summoner_ids.index(x), len(summoner_ids)), header2=True)



 
     
  if table=="match":
    add_match = ("INSERT IGNORE INTO matches "
               "(mapId, matchCreation, matchDuration, matchId, matchMode, matchType, matchVersion, platformId, queueType, region, season) " 
               "VALUES (%(mapId)s, %(matchCreation)s, %(matchDuration)s, %(matchId)s, %(matchMode)s, %(matchType)s, %(matchVersion)s, %(platformId)s, %(queueType)s, %(region)s, %(season)s)")

    add_match_participants = ("INSERT IGNORE INTO match_participants "
               "(matchId, championId, highestAchievedSeasonTier, participantId, profileIcon, matchHistoryUri, summonerName, summonerId, spell1Id, spell2Id, assists, champLevel, combatPlayerScore, deaths, doubleKills, firstBloodAssist, firstBloodKill, firstInhibitorAssist, firstInhibitorKill, firstTowerAssist, firstTowerKill, goldEarned, goldSpent, inhibitorKills, item0, item1, item2, item3, item4, item5, item6, killingSprees, kills, largestCriticalStrike, largestKillingSpree, largestMultiKill, magicDamageDealt, magicDamageDealtToChampions, magicDamageTaken, minionsKilled, neutralMinionsKilled, neutralMinionsKilledEnemyJungle, neutralMinionsKilledTeamJungle, nodeCapture, nodeCaptureAssist, nodeNeutralize, nodeNeutralizeAssist, objectivePlayerScore, pentaKills, physicalDamageDealt, physicalDamageDealtToChampions, physicalDamageTaken, quadrakills, sightWardsBoughtInGame, teamObjective, totalDamageDealt, totalDamageDealtToChampions, totalDamageTaken, totalHeal, totalPlayerScore, totalScoreRank, totalTimeCrowdControlDealt, totalUnitsHealed, towerKills, tripleKills, trueDamageDealt, trueDamageDealtToChampions, trueDamageTaken, unrealKills, visionWardsBoughtInGame, wardsKilled, wardsPlaced, winner, teamId, lane, role) " 
               "VALUES (%(matchId)s, %(championId)s, %(highestAchievedSeasonTier)s,  %(participantId)s, %(profileIcon)s, %(matchHistoryUri)s, %(summonerName)s, %(summonerId)s, %(spell1Id)s, %(spell2Id)s, %(assists)s, %(champLevel)s, %(combatPlayerScore)s, %(deaths)s, %(doubleKills)s, %(firstBloodAssist)s, %(firstBloodKill)s, %(firstInhibitorAssist)s, %(firstInhibitorKill)s, %(firstTowerAssist)s, %(firstTowerKill)s, %(goldEarned)s, %(goldSpent)s, %(inhibitorKills)s, %(item0)s, %(item1)s, %(item2)s, %(item3)s, %(item4)s, %(item5)s, %(item6)s, %(killingSprees)s, %(kills)s, %(largestCriticalStrike)s, %(largestKillingSpree)s, %(largestMultiKill)s, %(magicDamageDealt)s, %(magicDamageDealtToChampions)s, %(magicDamageTaken)s, %(minionsKilled)s, %(neutralMinionsKilled)s, %(neutralMinionsKilledEnemyJungle)s, %(neutralMinionsKilledTeamJungle)s, %(nodeCapture)s, %(nodeCaptureAssist)s, %(nodeNeutralize)s, %(nodeNeutralizeAssist)s, %(objectivePlayerScore)s, %(pentaKills)s, %(physicalDamageDealt)s, %(physicalDamageDealtToChampions)s, %(physicalDamageTaken)s, %(quadrakills)s, %(sightWardsBoughtInGame)s, %(teamObjective)s, %(totalDamageDealt)s, %(totalDamageDealtToChampions)s, %(totalDamageTaken)s, %(totalHeal)s, %(totalPlayerScore)s, %(totalScoreRank)s, %(totalTimeCrowdControlDealt)s, %(totalUnitsHealed)s, %(towerKills)s, %(tripleKills)s, %(trueDamageDealt)s, %(trueDamageDealtToChampions)s, %(trueDamageTaken)s, %(unrealKills)s, %(visionWardsBoughtInGame)s, %(wardsKilled)s, %(wardsPlaced)s, %(winner)s, %(teamId)s, %(lane)s, %(role)s)")

    add_match_participant_rune = ("INSERT IGNORE INTO match_participant_runes "
               "(matchId, summonerId, rank, runeId)"
               "VALUES (%(matchId)s, %(summonerId)s, %(rank)s, %(runeId)s)")
              
    add_match_participant_mastery = ("INSERT IGNORE INTO match_participant_masteries "
               "(matchId, summonerId, rank, masteryId)"
               "VALUES (%(matchId)s, %(summonerId)s, %(rank)s, %(masteryId)s)")

    add_match_participant_delta = ("INSERT IGNORE INTO match_participant_deltas "
               "(matchId, summonerId, deltaName, deltaTimeframe, value)"
               "VALUES (%(matchId)s, %(summonerId)s, %(deltaName)s, %(deltaTimeframe)s, %(value)s)")
                
    add_match_teams = ("INSERT IGNORE INTO match_teams "
                "(matchId, teamId, baronKills, dominionVictoryScore, dragonKills, firstBaron, firstBlood, firstDragon, firstInhibitor, firstRiftHerald, firstTower, inhibitorKills, riftHeraldKills, towerKills, vilemawKills, winner) " 
                "VALUES (%(matchId)s, %(teamId)s, %(baronKills)s, %(dominionVictoryScore)s, %(dragonKills)s, %(firstBaron)s, %(firstBlood)s, %(firstDragon)s, %(firstInhibitor)s, %(firstRiftHerald)s, %(firstTower)s, %(inhibitorKills)s, %(riftHeraldKills)s, %(towerKills)s, %(vilemawKills)s, %(winner)s	)")
   
    add_match_bans = ("INSERT IGNORE INTO match_team_bans "
                "(matchId, teamId, pickTurn, championId) " 
                "VALUES (%(matchId)s, %(teamId)s, %(pickTurn)s, %(championId)s)")
          
    add_match_timeline = ("INSERT IGNORE INTO match_timeline "
                "(matchId, summonerId, timestamp, currentGold, positionX, positionY, minionsKilled, level, jungleMinionsKilled, totalGold, dominionScore, participantId, xp, teamScore, timelineInterval) "
                "VALUES (%(matchId)s, %(summonerId)s, %(timestamp)s, %(currentGold)s, %(positionX)s, %(positionY)s, %(minionsKilled)s, %(level)s, %(jungleMinionsKilled)s, %(totalGold)s, %(dominionScore)s, %(participantId)s, %(xp)s, %(teamScore)s, %(timelineInterval)s )")
    add_match_timeline_event_assist = ("INSERT IGNORE INTO match_timeline_events_assist "
                 "(eventId, matchId, assistId)"
                 "VALUES (%(eventId)s, %(matchId)s, %(assistId)s)")
    if allow_updates==True:
     add_match_timeline_event =  ("REPLACE INTO match_timeline_events "
                 "(eventId, matchId, summonerId, timelineTimestamp, eventTimestamp, ascendedType, assistingParticipants, buildingType, creatorId, eventType, itemAfter, itemBefore, itemId, killerId, laneType, levelUpType, monsterType, pointCaptured, positionX, positionY, skillSlot, teamId, towerType, victimId, wardType) "
                 "VALUES (%(eventId)s, %(matchId)s, %(summonerId)s, %(timelineTimestamp)s, %(eventTimestamp)s, %(ascendedType)s, %(assistingParticipants)s, %(buildingType)s, %(creatorId)s, %(eventType)s, %(itemAfter)s, %(itemBefore)s, %(itemId)s, %(killerId)s, %(laneType)s, %(levelUpType)s, %(monsterType)s, %(pointCaptured)s, %(positionX)s, %(positionY)s, %(skillSlot)s, %(teamId)s, %(towerType)s, %(victimId)s, %(wardType)s ) ")
  

    else:  
     add_match_timeline_event =  ("INSERT IGNORE INTO match_timeline_events "
                 "(eventId, matchId, summonerId, timelineTimestamp, eventTimestamp, ascendedType, assistingParticipants, buildingType, creatorId, eventType, itemAfter, itemBefore, itemId, killerId, laneType, levelUpType, monsterType, pointCaptured, positionX, positionY, skillSlot, teamId, towerType, victimId, wardType) "
                 "VALUES (%(eventId)s, %(matchId)s, %(summonerId)s, %(timelineTimestamp)s, %(eventTimestamp)s, %(ascendedType)s, %(assistingParticipants)s, %(buildingType)s, %(creatorId)s, %(eventType)s, %(itemAfter)s, %(itemBefore)s, %(itemId)s, %(killerId)s, %(laneType)s, %(levelUpType)s, %(monsterType)s, %(pointCaptured)s, %(positionX)s, %(positionY)s, %(skillSlot)s, %(teamId)s, %(towerType)s, %(victimId)s, %(wardType)s )")
  


    
    self.print_stuff("Updating Match Tables.", header1 = True)
    if allow_updates != True:
     self.cursor.execute("SELECT matchId FROM matches")
     existing_matches_raw = self.cursor.fetchall()
     existing_matches = [unicode(x[0]) for x in existing_matches_raw]
#     for x in existing_matches_raw:
#      for y in x:
#       existing_matches.append(y)




#     for x in existing_timelines_raw:
#      for y in x:
#       existing_timelines.append(y)
      
    if timeline_update == True:
     self.cursor.execute("SELECT DISTINCT(matchId) FROM match_timeline")
     existing_timelines_raw = self.cursor.fetchall()
     existing_timelines = [unicode(x[0]) for x in existing_timelines_raw]
     self.print_stuff("Overriding matchIds, updating timelines for existing matches.")     
     match_ids = list(set(existing_matches)-set(existing_timelines))
     
    elif(matchIds==False):
     self.print_stuff("No list of match ids, defaulting to search team_history.")
     self.cursor.execute("SELECT gameId FROM team_history" )         
     match_ids_raw = self.cursor.fetchall()
   
     match_ids = [unicode(x[0]) for x in match_ids_raw] 
     if allow_updates != True :
      match_ids = list(set(match_ids)-set(existing_matches))
   
#      for x in match_ids_raw:
#       for y in x:
#        match_ids.append(y)
     
    else:
     self.print_stuff("Given list of match ids.")
     match_ids = matchIds
     if allow_updates != True :
      match_ids = list(set(match_ids)-set(existing_matches))
 
 #    print match_ids
    teams_data = []
    
    for x in match_ids:
     finished = False 
     cur_match_raw = []

     while finished == False:
      self.wait()
      try: 
       cur_match_raw = self.w.match.by_id( self._region, x)
      except HTTPError as err:
        if err.response.status_code == 429:
            print('We should retry in {} seconds.'.format(e.headers['Retry-After']))
#             print('this retry-after is handled by default by the RiotWatcher library')
#             print('future requests wait until the retry-after time passes')
        elif err.response.status_code == 404:
            print('Match not found.')
        else:
            raise
#       except riotwatcher.riotwatcher.LoLException as err:
#        if (str(err) == "Too many requests" and 'Retry-After' in err.headers) or str(err) == "Unauthorized" or str(err) == "Blacklisted key":
#  #         print "New Key" 
#          drop = False
#          if str(err) == "Blacklisted key":
#           self.print_stuff("Blacklisted key, using new key, dropping current.")
#           drop = True
#          if str(err) == "Unauthorized":
#           self.print_stuff("Unauthorized, using new key")
#          if len(self.keydict)==1:
#           self.print_stuff("Too many requests, not enough keys.", error=True)
#           if hangwait == False:
#            break 
#           else:
#            time.sleep(0.5)
#          self.new_key(drop=drop)
#        elif (str(err) == "Too many requests" and 'Retry-After' not in err.headers):
#         time.sleep(0.01) 
#        else:
#         self.print_stuff("%s, Match: %s -- Request" % (str(err), x),error=True)
#         break
 #      except:
 #       print "Other Error"
      
      else:
       finished = True
    
    
     if cur_match_raw:
     
      
      cur_match = {}
      
      for y in ["mapId", "matchCreation", "matchDuration", "matchId", "matchMode", "matchType", "matchVersion", "platformId", "queueType", "region", "season"]:
       try:
        cur_match[y] = cur_match_raw[y]

       except:
        cur_match[y] = None
    
  #     print cur_match
      if self.SQL == True:
          try:
           self.cursor.execute(add_match, cur_match)
          except mysql.connector.Error as err:

            if err.errno != 1062 or self.suppress_duplicates == False:
             self.print_stuff("%s, Match: %s -- Match" % (err.errno, x), error= True)
      #       print add_team % cur_team
          else:
            self.print_stuff("Updated Match")
      else:
        with open(path.join(os.getcwd(), 'matchfiles/matches_%s.tsv' % self.mat), "ab+") as csvfile:
            swriter = csv.DictWriter(csvfile, restval = 'NULL', delimiter='\t',
                            quotechar='|', quoting=csv.QUOTE_MINIMAL, fieldnames=sorted(cur_match.keys()))
            maxsize = 150000000
            size = os.fstat(csvfile.fileno()).st_size 
            if size > maxsize:
                self.mat = self.mat + 1
            swriter.writerow(cur_match)
      all_match_teams = []
      all_match_bans = []
    
  #     print cur_match_raw['teams']
      for y in cur_match_raw['teams']:
       cur_match_teams = {}
       for z in ("bans", "matchId", "teamId", "baronKills", "dominionVictoryScore", "dragonKills", "firstBaron", "firstBlood", "firstDragon", "firstInhibitor", "firstRiftHerald", "firstTower", "inhibitorKills", "riftHeraldKills", "towerKills", "vilemawKills", "winner"):
        if z == "bans" and "bans" in y:
         for s in y['bans']:
          cur_match_bans = {}
          for t in s:
           cur_match_bans[t] = s[t]
          cur_match_bans['matchId'] = x
          cur_match_bans['teamId'] = y['teamId']
          all_match_bans.append(cur_match_bans)

     
        else:
         try:
          cur_match_teams[z] = y[z]
         except:
          cur_match_teams[z] = None
     
       cur_match_teams['matchId'] = x
       all_match_teams.append(cur_match_teams)
      
      if self.SQL == True:
          try:
           self.cursor.executemany(add_match_teams, all_match_teams)
          except mysql.connector.Error as err:
           if err.errno != 1062 or self.suppress_duplicates == False:
            self.print_stuff("%s, Match: %s -- Teams" % (err.errno, x), error=True)
          else:
           self.print_stuff("Updated Match-Teams")
          try:
           self.cursor.executemany(add_match_bans, all_match_bans)
          except mysql.connector.Error as err:
           if err.errno != 1062 or self.suppress_duplicates == False:
            self.print_stuff("%s, Match: %s -- Bans" % (err.errno, x), error=True)
          else:
           self.print_stuff("Updated Match-Bans")
      else: 
        with open(path.join(os.getcwd(), 'matchfiles/match_teams_%s.tsv' % self.mteam), "ab+") as csvfile:
            fieldnames = ['matchId','teamId','baronKills','dominionVictoryScore','dragonKills','firstBaron',
                        'firstBlood','firstDragon','firstInhibitor','firstRiftHerald','firstTower','inhibitorKills',
                        'riftHeraldKills','towerKills','vilemawKills','winner']
            swriter = csv.DictWriter(csvfile, restval = 'NULL', delimiter='\t',
                            quotechar='|', quoting=csv.QUOTE_MINIMAL, fieldnames=fieldnames)
            maxsize = 15000000 
            size = os.fstat(csvfile.fileno()).st_size
            if size > maxsize:
                self.mteam = self.mteam + 1
            for row in all_match_teams:
                if 'bans' in row:
                    if row['bans'] == "":
                        del row['bans']
                        swriter.writerow(row)
                    elif row['bans'] == None:
                        del row['bans']
                        swriter.writerow(row)
                    else:
                        with open(path.join(os.getcwd(), 'matchfiles/match_teams_broke_%s.tsv' % self.mteam), "ab+") as csvfile2:
                            swriter2 = csv.DictWriter(csvfile2, delimiter = '\t',
                                quotechar='|', quoting=csv.QUOTE_MINIMAL, fieldnames=row.keys())
                            swriter2.writerow(row)
                else:
                    swriter.writerow(row)
        if len(all_match_bans)>0:   
            with open(path.join(os.getcwd(), 'matchfiles/match_team_bans_%s.tsv' % self.mtb), "ab+") as csvfile:
                fieldnames = ['matchId','teamId','pickTurn','championId']
                swriter = csv.DictWriter(csvfile, restval = 'NULL', delimiter='\t',
                                quotechar='|', quoting=csv.QUOTE_MINIMAL, fieldnames=fieldnames)
                maxsize = 15000000 
                size = os.fstat(csvfile.fileno()).st_size
                if size > maxsize:
                    self.mtb = self.mtb + 1
                for row in all_match_bans:
                    swriter.writerow(row)         
       
       
           
  #     print cur_match_raw
      cur_match_participants_raw = cur_match_raw["participants"]
    
    
     if cur_match_raw:
      cur_match_pi = {}
      for y in cur_match_raw["participantIdentities"]:
       try:
        cur_match_pi[y["participantId"]] = y["player"]
       except:
        self.print_stuff("No SummonerIds -- %s" % (x), error=True)
        cur_match_raw = False
    
    
      
      
      
      
      
     if timeline == True and cur_match_raw:  
      try: 
       cur_match_raw["timeline"] = self.w.match.timeline_by_match(self._region, x)
      except HTTPError as err:
        if err.response.status_code == 429:
            print('We should retry in {} seconds.'.format(e.headers['Retry-After']))
#             print('this retry-after is handled by default by the RiotWatcher library')
#             print('future requests wait until the retry-after time passes')
        elif err.response.status_code == 404:
            print('Timeline not found.')
        else:
            raise
      if "timeline" in cur_match_raw:
       cur_match_timeline_raw = cur_match_raw["timeline"]   
       intervals = cur_match_timeline_raw["frameInterval"]

       for y in cur_match_timeline_raw["frames"]:

        cur_timeline = []


   #      print cur_time
   #      print y
        for z in y["participantFrames"]:
         cur_participant_timeline = {}
      
   #       print "z %s" % z
         for s in ["currentGold", "position", "minionsKilled", "level", "jungleMinionsKilled", "totalGold", "dominionScore", "participantId", "xp", "teamScore", "timelineInterval"]:
          if s != "position":
           try:
            cur_participant_timeline[s] = y["participantFrames"][z][s]
           except:
            cur_participant_timeline[s] = None
          else:
           try:
            cur_participant_timeline['positionX'] = y["participantFrames"][z][s]["x"]
           except:
            cur_participant_timeline['positionX'] = None
           try:
            cur_participant_timeline['positionY'] = y["participantFrames"][z][s]["y"]
           except:
            cur_participant_timeline['positionY'] = None
        
         cur_participant_timeline['matchId'] = x
         cur_participant_timeline['summonerId'] = cur_match_pi[int(z)]["summonerId"]
         cur_participant_timeline['timestamp'] = y["timestamp"]
         cur_participant_timeline['timelineInterval'] = intervals
         cur_timeline.append(cur_participant_timeline)
      
     
   #      timeline_events = []
  #       print x
        if "events" in y:
         timeline_events = []
         assists = []   


         for z in y["events"]:
          cur_timeline_event = {}
   #        print z

          for s in ["ascendedType", "assistingParticipants", "buildingType", "creatorId", "eventType", "itemAfter", "itemBefore", "itemId", "killerId", "laneType", "levelUpType", "monsterType", "pointCaptured", "position", "skillSlot", "teamId", "towerType", "victimId", "wardType"]:
        
       
           if s=="assistingParticipants":
        
            if "assistingParticipantIds" in z:
             cur_timeline_event["assistingParticipants"]=True
             for t in z["assistingParticipantIds"]:
              cur_assists = {}
              cur_assists["eventId"] = "%s - %s" % (cur_match_timeline_raw["frames"].index(y),  y["events"].index(z))
              cur_assists["matchId"] = x
              cur_assists["assistId"] = cur_match_pi[t]["summonerId"]
              assists.append(cur_assists)

          
          
            else:
             cur_timeline_event["assistingParticipants"]=False
           elif s=="creatorId" or s=="killerId" or s=="victimId":
            try: 
             z[s]
            except:
             cur_timeline_event[s] = None
            else:
             if z[s] == 0:
              if s == "killerId":
               cur_timeline_event[s] = "minion"
              if s == "creatorId":
  #              we're not sure why creatorId would be set to 0, and no one online is 100% sure either. 
               cur_timeline_event[s] = "undefined"
             else:
  #             print z
              cur_timeline_event[s] = cur_match_pi[z[s]]["summonerId"]
           elif s == "position":
            try:
             cur_timeline_event['positionX'] = z[s]["x"]
            except:
             cur_timeline_event['positionX'] = None
            try:
             cur_timeline_event['positionY'] = z[s]["y"]
            except:
             cur_timeline_event['positionY'] = None
           else:
            try:
             z[s]
            except:
             cur_timeline_event[s] = None
            else:
             cur_timeline_event[s] = z[s]
          cur_timeline_event["eventId"] = "%s - %s" % (cur_match_timeline_raw["frames"].index(y),  y["events"].index(z))
          cur_timeline_event["matchId"] = x
          if "participantId" in z:
           if z["participantId"] != 0:
            cur_timeline_event["summonerId"] =  cur_match_pi[z["participantId"]]["summonerId"]
           else:
            cur_timeline_event["summonerId"] = None
          else:
           cur_timeline_event["summonerId"] = None
          cur_timeline_event["timelineTimestamp"] = y["timestamp"]
          cur_timeline_event["eventTimestamp"] = z["timestamp"]
          timeline_events.append(cur_timeline_event)
       
   #        print cur_timeline_event

   #       print timeline_events 
         if self.SQL == True:
             try:
              self.cursor.executemany(add_match_timeline_event, timeline_events)

             except mysql.connector.Error as err:
              if err.errno != 1062 or self.suppress_duplicates == False:
               self.print_stuff("%s, Match: %s, Timeframe: %s-- Timeline-Events" % (err.errno, x, cur_match_timeline_raw["frames"].index(y)), error=True)
             else:
              self.print_stuff("Updated Timeline-Events")
         else: 
            with open(path.join(os.getcwd(), 'matchfiles/match_timeline_events_%s.tsv' % self.mte), "ab+") as csvfile:

                fieldnames = ['eventId','matchId','summonerId','timelineTimestamp','eventTimestamp','ascendedType',
                                'assistingParticipants','buildingType','creatorId','eventType','itemAfter','itemBefore','itemId',
                                'killerId','laneType','levelUpType','monsterType','pointCaptured','positionX','positionY','skillSlot',
                                'teamId','towerType','victimId','wardType']
                swriter = csv.DictWriter(csvfile, restval = 'NULL', delimiter='\t',
                                quotechar='|', quoting=csv.QUOTE_MINIMAL, fieldnames=fieldnames)
                maxsize = 150000000
                size = os.fstat(csvfile.fileno()).st_size 
                if size > maxsize:
                    self.mte = self.mte + 1
                for row in timeline_events:
                    swriter.writerow(row)            
         if assists != []:
             if self.SQL == True:
                  try:
           #         print assists
                   self.cursor.executemany(add_match_timeline_event_assist, assists)

                  except mysql.connector.Error as err:
                   if err.errno != 1062 or self.suppress_duplicates == False:
                    self.print_stuff("%s, Match: %s, Timeframe: %s-- Timeline-Events-Assists" % (err.errno, x, cur_match_timeline_raw["frames"].index(y)),error=True)
                  else:
                   self.print_stuff("Updated Timeline-Assists")
             else:
                with open(path.join(os.getcwd(), 'matchfiles/match_timeline_events_assist_%s.tsv' % self.mtea), "ab+") as csvfile:
                    fieldnames = ['eventId', 'matchId', 'assistId']
                    swriter = csv.DictWriter(csvfile, restval = 'NULL', delimiter='\t',
                                    quotechar='|', quoting=csv.QUOTE_MINIMAL, fieldnames = fieldnames)
                    maxsize = 150000000
                    size = os.fstat(csvfile.fileno()).st_size 
                    if size > maxsize:
                        self.mtea = self.mtea + 1
                    for row in assists:
                        swriter.writerow(row)   
            #       stop
    
       
        if self.SQL == True:
            try:
             self.cursor.executemany(add_match_timeline, cur_timeline)
            except mysql.connector.Error as err:
             if err.errno != 1062 or self.suppress_duplicates == False:
              self.print_stuff("%s, Match: %s, Timeframe: %s-- Timeline" % (err.errno, x, cur_match_timeline_raw["frames"].index(y)), error=True)
            else:
             self.print_stuff("Updated Timeline")
        else:
            with open(path.join(os.getcwd(), 'matchfiles/match_timeline_%s.tsv' % self.mt), "ab+") as csvfile:
                fieldnames = ['matchId','summonerId','timestamp','currentGold','positionX','positionY',
                                'minionsKilled','level','jungleMinionsKilled','totalGold','dominionScore','participantId',
                                'xp','teamScore','timelineInterval']
                maxsize = 150000000 
                size = os.fstat(csvfile.fileno()).st_size
                if size > maxsize:
                    self.mt = self.mt + 1
                swriter = csv.DictWriter(csvfile, restval = 'NULL', delimiter='\t',
                                quotechar='|', quoting=csv.QUOTE_MINIMAL, fieldnames=fieldnames)
                for row in cur_timeline:
                    swriter.writerow(row)   
      
      
      
      
   #       if killerid == 0, Minion

       self.print_stuff("Finished Timeline")
      else:
       self.print_stuff("No Timeline Data; %s" % (x))
    
     if cur_match_raw:
      runes = []
      masteries = []
      deltas = []
      all_match_participants = []
      for y in cur_match_participants_raw:
       cur_match_participant = {}
  #      print y
  #      break
       for z in cur_match_pi[y["participantId"]]:
      
        cur_match_participant[z] = cur_match_pi[y["participantId"]][z]
  #       print cur_match_participant
       cur_match_participant["matchId"] = x
       for z in y:
  #       print z
        if isinstance(y[z], list) or isinstance(y[z], dict):
       
         if z == "runes":
          cur_rune = {}
          cur_rune["matchId"] = x
          cur_rune["summonerId"] = cur_match_pi[y["participantId"]]["summonerId"]
          for r in y[z]:
           for r2 in r:
            cur_rune[r2] = r[r2]
            runes.append(cur_rune)
     
         
         
         elif z == "masteries": 
          cur_mastery = {}  
          cur_mastery["matchId"] = x
          cur_mastery["summonerId"] = cur_match_pi[y["participantId"]]["summonerId"]
          for r in y[z]: 
           for r2 in r:
            cur_mastery[r2] = r[r2]
            masteries.append(cur_mastery)
            
           
#            try:
#             self.cursor.execute(add_match_participant_mastery, cur_mastery)
#            except mysql.connector.Error as err:
#             if err.errno != 1062 or self.suppress_duplicates == False:
#              self.print_stuff("%s, Match: %s, Player: %s -- Mastery" % (err.errno, x, cur_match_pi[y["participantId"]]["summonerId"]),error=True)
#    #          print add_team % cur_team
#            else:
#             self.print_stuff("Updated Mastery")     
       
         elif z == "stats": 
          for r in ["assists", "champLevel", "combatPlayerScore", "deaths", "doubleKills", "firstBloodAssist", "firstBloodKill", "firstInhibitorAssist", "firstInhibitorKill", "firstTowerAssist", "firstTowerKill", "goldEarned", "goldSpent", "inhibitorKills", "item0", "item1", "item2", "item3", "item4", "item5", "item6", "killingSprees", "kills", "largestCriticalStrike", "largestKillingSpree", "largestMultiKill", "magicDamageDealt", "magicDamageDealtToChampions", "magicDamageTaken", "minionsKilled", "neutralMinionsKilled", "neutralMinionsKilledEnemyJungle", "neutralMinionsKilledTeamJungle", "nodeCapture", "nodeCaptureAssist", "nodeNeutralize", "nodeNeutralizeAssist", "objectivePlayerScore", "pentaKills", "physicalDamageDealt", "physicalDamageDealtToChampions", "physicalDamageTaken", "quadrakills", "sightWardsBoughtInGame", "teamObjective", "totalDamageDealt", "totalDamageDealtToChampions", "totalDamageTaken", "totalHeal", "totalPlayerScore", "totalScoreRank", "totalTimeCrowdControlDealt", "totalUnitsHealed", "towerKills", "tripleKills", "trueDamageDealt", "trueDamageDealtToChampions", "trueDamageTaken", "unrealKills", "visionWardsBoughtInGame", "wardsKilled", "wardsPlaced", "winner"]:
           try:
            cur_match_participant[r] = y[z][r]
           except:
            cur_match_participant[r] = None
         elif z == "timeline":
          for r in y[z]:
           if r=="role" or r=="lane":
            cur_match_participant[r] = y[z][r]
           else:
            cur_delta = {}
            for r2 in y[z][r]:
             cur_delta["matchId"] = x
             cur_delta["summonerId"] = cur_match_pi[y["participantId"]]["summonerId"]
             cur_delta["deltaName"] = r
             cur_delta["deltaTimeframe"] = r2
             cur_delta["value"] = y[z][r][r2]
             deltas.append(cur_delta)

             ##OLD
             # try:
#               self.cursor.execute(add_match_participant_delta, cur_delta)
#              except mysql.connector.Error as err:
#               if err.errno != 1062 or self.suppress_duplicates == False:
#                self.print_stuff("%s, Match: %s, Player: %s -- Delta" % (err.errno, x, cur_match_pi[y["participantId"]]["summonerId"]),error=True)
#      #          print add_team % cur_team
#              else:
#               self.print_stuff("Updated Participant Deltas") 
       
         else:
          self.print_stuff("Broken: %s" % z)
       
       
        else:
         cur_match_participant[z] = y[z]
     
  #      print cur_match_participant_raw     
  #      if feedback == "all":
  #       print cur_match_participant["nodeCapture"]
       all_match_participants.append(cur_match_participant)
      if self.SQL == True:
          try:
           self.cursor.executemany(add_match_participant_rune, runes)
          except mysql.connector.Error as err:
           if err.errno != 1062 or self.suppress_duplicates == False:
            self.print_stuff("%s, Match: %s -- Rune" % (err.errno, x),error=True)
    #          print add_team % cur_team
          else:
           self.print_stuff("Updated Rune")      
      
          try:
           self.cursor.executemany(add_match_participant_mastery, masteries)
          except mysql.connector.Error as err:
           if err.errno != 1062 or self.suppress_duplicates == False:
            self.print_stuff("%s, Match: %s -- Mastery" % (err.errno, x),error=True)
    #          print add_team % cur_team
          else:
           self.print_stuff("Updated Mastery")    
          try:
           self.cursor.executemany(add_match_participant_delta, deltas)
          except mysql.connector.Error as err:
           if err.errno != 1062 or self.suppress_duplicates == False:
            self.print_stuff("%s, Match: %s -- Delta" % (err.errno, x),error=True)
    #          print add_team % cur_team
          else:
           self.print_stuff("Updated Participant Deltas") 

          try: 
           self.cursor.executemany(add_match_participants, all_match_participants)
     #       print "Try"
    
          except mysql.connector.Error as err:
           if err.errno != 1062 or self.suppress_duplicates == False:
            self.print_stuff("%s, Match: %s-- Participant" % (err.errno, x), error=True)
     #       print add_team % cur_team
          else:
           self.print_stuff("Updated Match-Participants")

      else: 
        with open(path.join(os.getcwd(), 'matchfiles/match_participant_runes_%s.tsv' % self.mpr), "ab+") as csvfile:
            fieldnames = ['matchId','summonerId','rank','runeId']
            swriter = csv.DictWriter(csvfile, restval = 'NULL', delimiter='\t',
                            quotechar='|', quoting=csv.QUOTE_MINIMAL, fieldnames=fieldnames)
            maxsize = 150000000 
            size = os.fstat(csvfile.fileno()).st_size
            if size > maxsize:
                self.mpr = self.mpr + 1                            
            for row in runes:
                swriter.writerow(row)   
        with open(path.join(os.getcwd(), 'matchfiles/match_participant_masteries_%s.tsv' % self.mpm), "ab+") as csvfile:
            fieldnames = ['matchId','summonerId','rank','masteryId']
            swriter = csv.DictWriter(csvfile, restval = 'NULL', delimiter='\t',
                            quotechar='|', quoting=csv.QUOTE_MINIMAL, fieldnames=fieldnames)
            maxsize = 150000000 
            size = os.fstat(csvfile.fileno()).st_size
            if size > maxsize:
                self.mpm = self.mpm + 1
            for row in masteries:
                swriter.writerow(row)   
        with open(path.join(os.getcwd(), 'matchfiles/match_participant_deltas_%s.tsv' % self.mpd), "ab+") as csvfile:
            fieldnames = ['matchId','summonerId','deltaName','deltaTimeframe', 'value']
            swriter = csv.DictWriter(csvfile, restval = 'NULL', delimiter='\t',
                            quotechar='|', quoting=csv.QUOTE_MINIMAL, fieldnames=fieldnames)
            maxsize = 150000000 
            size = os.fstat(csvfile.fileno()).st_size
            if size > maxsize:
                self.mpd = self.mpd + 1
            for row in deltas:
                swriter.writerow(row)   
        with open(path.join(os.getcwd(), 'matchfiles/match_participants_%s.tsv' % self.mp), "ab+") as csvfile:
            fieldnames = ['matchId', 'championId','highestAchievedSeasonTier',
                            'participantId','profileIcon','matchHistoryUri','summonerName','summonerId',
                            'spell1Id','spell2Id','assists','champLevel','combatPlayerScore','deaths','doubleKills',
                            'firstBloodAssist','firstBloodKill','firstInhibitorAssist',
                            'firstInhibitorKill','firstTowerAssist','firstTowerKill','goldEarned','goldSpent',
                            'inhibitorKills','item0','item1','item2','item3','item4','item5','item6','killingSprees','kills',
                            'largestCriticalStrike','largestKillingSpree','largestMultiKill','magicDamageDealt',
                            'magicDamageDealtToChampions','magicDamageTaken','minionsKilled','neutralMinionsKilled',
                            'neutralMinionsKilledEnemyJungle','neutralMinionsKilledTeamJungle','nodeCapture','nodeCaptureAssist',
                            'nodeNeutralize','nodeNeutralizeAssist','objectivePlayerScore','pentaKills','physicalDamageDealt',
                            'physicalDamageDealtToChampions','physicalDamageTaken','quadrakills','sightWardsBoughtInGame','teamObjective',
                            'totalDamageDealt','totalDamageDealtToChampions','totalDamageTaken','totalHeal','totalPlayerScore',
                            'totalScoreRank','totalTimeCrowdControlDealt','totalUnitsHealed','towerKills','tripleKills',
                            'trueDamageDealt','trueDamageDealtToChampions','trueDamageTaken','unrealKills','visionWardsBoughtInGame',
                            'wardsKilled','wardsPlaced','winner','teamId','lane','role']
            swriter = csv.DictWriter(csvfile, restval = 'NULL', delimiter='\t',
                            quotechar='|', quoting=csv.QUOTE_MINIMAL, fieldnames=fieldnames)
            maxsize = 150000000 
            size = os.fstat(csvfile.fileno()).st_size
            if size > maxsize:
                self.mp = self.mp + 1
            for row in all_match_participants:
                row['summonerName'] = unicode(row['summonerName']).encode("utf-8")
                swriter.writerow(row)  
         ##OLD
#        try: 
#         self.cursor.execute(add_match_participants, cur_match_participant)
#   #       print "Try"
#      
#        except mysql.connector.Error as err:
#         if err.errno != 1062 or self.suppress_duplicates == False:
#          self.print_stuff("%s, Match: %s, Player: %s -- Participant" % (err.errno, x, cur_match_pi[y["participantId"]]["summonerId"]), error=True)
#   #       print add_team % cur_team
#        else:
#         self.print_stuff("Updated Match-Participants")
        
        
        if teamIds != False:
            add_team_history = ("INSERT IGNORE INTO team_history "
                 "(fullId, assists, date, deaths, gameId, gameMode, invalid, kills, mapId, opposingTeamKills, opposingTeamName, win)" 
                 "VALUES (%(fullId)s, %(assists)s, %(date)s, %(deaths)s, %(gameId)s, %(gameMode)s, %(invalid)s, %(kills)s, %(mapId)s, %(opposingTeamKills)s, %(opposingTeamName)s, %(win)s)" )
            all_teams = {}
            ## we have to figure out how we're gonna get these fields, 
        
            all_teams['gameId'] = int(x)
            all_teams['gameMode'] = cur_match_raw['matchMode']
            all_teams['date'] = cur_match_raw['matchCreation']
            all_teams['mapId'] = cur_match_raw['mapId']
            all_teams['fullId'] = teamIds[0]
        
            focus = {}
            opposing = {}
        
            for row in all_match_participants:
                if int(row['teamId']) == int(teamIds[1]):
                    for cur in ['assists', 'deaths', 'kills']:
                        if cur in focus:
                            focus[cur] = focus[cur] + row[cur]
                        else:
                            focus[cur] = row[cur]
                    focus['win'] = row['winner']
                else:
                    if 'kills' in opposing:
                        opposing['kills'] = opposing['kills'] + row['kills']
                    else:
                        opposing['kills'] = row['kills']
    #             y['stats']
#             print focus, opposing
            all_teams['opposingTeamKills'] = opposing['kills']
            all_teams['assists'] = focus['assists']
            all_teams['deaths'] = focus['deaths']
            all_teams['kills'] = focus['kills']
            all_teams['win'] = focus['win']
#             self.cursor.execute("SELECT name FROM team WHERE fullId = '%s' " % teamId[2])
#             teamname = self.cursor.fetchall()
#             all_teams['opposingTeamName'] = [x[0] for x in teamname][0]
            finished = False
            attempt = 0
            failed = False
            all_teams['opposingTeamName'] = teamIds[2]
            ## we no longer have a "Teams" api.
#             while finished == False:
#                 if attempt < 50:
#                     try:
#                         oppteams_data = self.w.get_teams([teamIds[2]])
#                         all_teams['opposingTeamName'] = oppteams_data[teamIds[2]]['name']
#                         finished = True
#                     except:
#                         attempt += 1
#                         pass
#                 else:
#                     failed = True
#                     finished = True
#             if failed == True:
#                 with open(path.join(os.getcwd(), 'failed_teamnames.tsv'), "ab+") as csvfile:
#                     fieldnames = ['matchId']
#                     swriter = csv.DictWriter(csvfile, restval = 'NULL', delimiter='\t',
#                                     quotechar='|', quoting=csv.QUOTE_MINIMAL, fieldnames=fieldnames)
#                     swriter.writerow({'matchId': int(x)})   
                    
            ##NOT SURE WHAT THIS MEANS YET
            all_teams['invalid'] = False
        
 
            try:
             self.cursor.execute(add_team_history, all_teams)
            #       print test_team
            except mysql.connector.Error as err:
             if err.errno != 1062 or suppress_duplicates == False:
              self.print_stuff("Error %s" % err.errno, error = True)
              print(all_teams)
            else:
             self.print_stuff("Updated Team-History")  
        
        
        
     else:
      if self.suppress_duplicates == False:
       self.print_stuff("Duplicate Match; %s" % (x))

     
     
     
    
       
     self.print_stuff("Finished %s of %s; %s" % (match_ids.index(x)+1,len(match_ids),x), header2=True)
     self.cnx.commit()
  
  
  
  if table=="stats":
   if summonerIds==False:
    self.print_stuff("No Ids provided, defaulting to searching Summoner List")
    self.cursor.execute("SELECT DISTINCT(summonerId) FROM summoner_list")
    summoner_ids_raw = self.cursor.fetchall()
    summoner_ids = [unicode(x[0]) for x in summoner_ids_raw] 
   else:
    summoner_ids = summonerIds
   if allow_updates == False:
    self.cursor.execute("SELECT DISTINCT(summonerId) FROM stats")
    existing_ids_raw = self.cursor.fetchall()
    existing_ids = [unicode(x[0]) for x in existing_ids_raw] 
    
    summoner_ids = list(set(summoner_ids) - set(existing_ids))
   
   for x in summoner_ids:
    self.get_aggstats(summoner_id=x, season=season)   
    if (summoner_ids.index(x)+1) % 10 == 0:
     self.print_stuff("Finished %s of %s" % (summoner_ids.index(x)+1, len(summoner_ids)), progress=True)
    
    
   self.print_stuff("Finished %s of %s" % (summoner_ids.index(x)+1, len(summoner_ids)), header2=True)
   
        
     
     

  self.cnx.commit()
  if self.use_curses == True :
   curses.endwin()
 
 
 def __exit__(self):
  if self.use_curses == True :
   curses.endwin()
  self.cnx.commit()
  self.cursor.close()
  self.cnx.close()
  if ssh == True:
   credentials.server.stop()
  self.reactor.stop()




## functions for actual use:
# update_table("iterate",iteratestart=10000, iterate=10000, checkTeams=True, suppress_duplicates=True)
# start_time = time.time()

# update_table("match", matchIds=[1976289359], suppress_duplicates=True)
# update_table("membertiers", matchIds=[2044253864,1976289359], suppress_duplicates=True)
# update_table("checkteams", feedback="all", suppress_duplicates=True)
# print "Took", time.time() - start_time, "to run"

# update_table("team", checkTeams=True, feedback="all", suppress_duplicates=True)
# 
# cursor.execute("SELECT gameId FROM team_history")
# matches= strip_to_list(cursor.fetchall())
# 
# 
# update_table("match", matchIds=matches, timeline=True, create=False, suppress_duplicates=True)
# update_table("membertiers", suppress_duplicates=True)



# update_table("checkteams", teamIds = ["TEAM-f8aac4c0-16de-11e5-87b6-c81f66dd45c9"])


# 
# cursor.execute("SELECT playerOrTeamID, queue FROM by_league LIMIT 10")
# test = cursor.fetchall()
# print test

   
   
# new_key()






### MAKE SURE ALL SCRIPT EXECUTIONS OCCUR BEFORE THE NEXT 5 LINES



# UPDATE matches SET queueType = 'TEAM_BUILDER_DRAFT_UNRANKED_5x5'  WHERE queueType = 'TEAM_BUILDER_DRAFT_U'  AND !isnull( queueType );
# CREATE INDEX id_team ON by_league (playerOrTeamId, team);


##For personal reference, just to keep track of alterations i've made after table creation, all are reflected in the table creation now though. 
# cursor.execute("ALTER TABLE team_roster MODIFY joinDate BIGINT DEFAULT NULL")

# cursor.execute("ALTER TABLE team MODIFY thirdLastJoinDate BIGINT DEFAULT NULL")
# cursor.execute("ALTER TABLE by_league DROP PRIMARY KEY")
# cursor.execute("ALTER TABLE by_league ADD CONSTRAINT id_queue PRIMARY KEY (`playerOrTeamId`, `queue`)")
# 
# 
# cursor.execute("ALTER TABLE team_history DROP PRIMARY KEY")
# cursor.execute("ALTER TABLE team_history ADD CONSTRAINT game_team PRIMARY KEY (`gameId`, `fullId`)")
# 
# cursor.execute("ALTER TABLE team_roster DROP PRIMARY KEY")
# cursor.execute("ALTER TABLE team_roster ADD CONSTRAINT player_team PRIMARY KEY (`playerId`, `teamId`)")
# cursor.execute("ALTER TABLE by_league DROP PRIMARY KEY, ADD CONSTRAINT id_queue PRIMARY KEY (`playerOrTeamId`, `queue`, `retrieved`)")
     
     
# cursor.execute("ALTER TABLE by_league MODIFY division varchar(5) NOT NULL")
# cursor.execute("DROP TABLE IF EXISTS by_league")
# cursor.execute("ALTER TABLE `by_league` ADD `retrieved` DATETIME NOT NULL")




#### FOR ANALYSIS
# cursor.execute("SELECT summonerId, matchId, teamId, winner, highestAchievedSeasonTier FROM match_participants")
# all_raw = cursor.fetchall()
# cursor.execute("SELECT t1.summonerId, t2.division, t2.league FROM match_participants AS t1 INNER JOIN by_league AS t2 ON t1.summonerId = t2.playerOrTeamId")
# people_raw = cursor.fetchall()
# 
# # people = {}
# people = []
# for x in people_raw:
#  people.append(unicode(x[0]))
# 
# all = []
# for x in all_raw:
#  if x[0] in people:
#   all.append([x[1], x[2], x[3], people_raw[people.index(x[0])][1], people_raw[people.index(x[0])][2]])
#  else:
#   all.append([x[1], x[2], x[3], 0, x[4]])
#  
# 
# 
# 
# with open('/Users/Matthew/Documents/League of Legends/testfile.csv', 'wb+') as csvfile:
#     swriter = csv.writer(csvfile, delimiter=' ',
#                             quotechar='|', quoting=csv.QUOTE_MINIMAL)
#     swriter.writerow(["matchId"] + ["teamId"] + ["winner"] + ["division"] + ["league"])
#     for x in all:
#      swriter.writerow(x)





# cursor.execute("SELECT summonerId, matchId, teamId, winner, highestAchievedSeasonTier, kills FROM match_participants")
# all_raw = cursor.fetchall()
# cursor.execute("SELECT t1.summonerId, t2.division, t2.league FROM match_participants AS t1 INNER JOIN by_league AS t2 ON t1.summonerId = t2.playerOrTeamId")
# people_raw = cursor.fetchall()
# 
# # people = {}
# people = []
# for x in people_raw:
#  people.append(x[0])
# 
# all = []
# for x in all_raw:
#  if x[0] in people:
#   all.append([x[1], x[2], x[3], x[5], people_raw[people.index(x[0])][1], people_raw[people.index(x[0])][2]])
#  else:
#   all.append([x[1], x[2], x[3], x[5], 0, x[4]])
#  
# 
# 
# 
# with open('/Users/Matthew/Documents/League of Legends/testfile.csv', 'wb+') as csvfile:
#     swriter = csv.writer(csvfile, delimiter=' ',
#                             quotechar='|', quoting=csv.QUOTE_MINIMAL)
#     swriter.writerow(["matchId"] + ["teamId"] + ["winner"] + ["kills"] + ["division"] + ["league"])
#     for x in all:
#      swriter.writerow(x)

