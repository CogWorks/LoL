### OLD CODE FOR HISTORIC PURPOSES. 
### THE FOLLOWING CODE WERE OPTIONS UNDER THE "update_table" FUNCTION, BUT NO LONGER WORK
### THIS IS DUE TO THE REMOVAL OF THE "TEAM API" 
   
     
   
        
  if table=="team":
   
    add_team = ("INSERT IGNORE INTO team "
                "(createDate, fullId, lastGameDate, lastJoinDate, lastJoinedRankedTeamQueueDate, modifyDate, name, secondLastJoinDate, status, tag, thirdLastJoinDate, averageGamesPlayed3v3, losses3v3, wins3v3, averageGamesPlayed5v5, losses5v5, wins5v5) " 
                "VALUES (%(createDate)s, %(fullId)s, %(lastGameDate)s, %(lastJoinDate)s, %(lastJoinedRankedTeamQueueDate)s, %(modifyDate)s, %(name)s, %(secondLastJoinDate)s, %(status)s, %(tag)s, %(thirdLastJoinDate)s, %(averageGamesPlayed3v3)s, %(losses3v3)s, %(wins3v3)s, %(averageGamesPlayed5v5)s, %(losses5v5)s, %(wins5v5)s) " )
 
    add_team_history = ("INSERT IGNORE INTO team_history "
             "(fullId, assists, date, deaths, gameId, gameMode, invalid, kills, mapId, opposingTeamKills, opposingTeamName, win)" 
             "VALUES (%(fullId)s, %(assists)s, %(date)s, %(deaths)s, %(gameId)s, %(gameMode)s, %(invalid)s, %(kills)s, %(mapId)s, %(opposingTeamKills)s, %(opposingTeamName)s, %(win)s)" )
   
    add_team_roster = ("INSERT IGNORE INTO team_roster "
             "(inviteDate, joinDate, playerId, status, isCaptain, teamId)"
             "VALUES (%(inviteDate)s, %(joinDate)s, %(playerId)s, %(status)s, %(isCaptain)s, %(teamId)s)")
    self.print_stuff("Updating Team Tables.", header1 = True)
    if(teamIds==False):
     self.print_stuff("No list of team ids, defaulting to search by_league")
     self.cursor.execute("SELECT DISTINCT(playerOrTeamId) FROM by_league WHERE team = True" )         
   

     team_ids_raw = self.cursor.fetchall()
   
     team_ids = [unicode(x[0]) for x in team_ids_raw] 
   
#      for x in team_ids_raw:
#       for y in x:
#        team_ids.append(y)
     
    else:
     self.print_stuff("Given list of team ids.")
     team_ids = teamIds
 
    teams_data = []

    for x in xrange(0,(int(len(team_ids)/10)+1)):

     stop = ((x+1)*10)


     if x == int(len(team_ids)/10) and (x+1)*10 != int(len(team_ids)/10):
      stop = (len(team_ids))
 #     print "%s, %s" % (x,stop)
     if x*10 == stop:
      continue


 #     print "%s, %s" % (x*10, stop)
     finished = False 
     service = 0
     while finished == False:
      self.wait()
      try: 
       teams_data = self.w.get_teams(team_ids[(x*10):stop])
     
      except riotwatcher.riotwatcher.LoLException as err:
       if (str(err) == "Too many requests" and 'Retry-After' in err.headers) or str(err) == "Service unavailable" or str(err) == "Unauthorized" or str(err) == "Internal server error" or str(err) == "Blacklisted key":
 #         print "New Key" 
         drop = False
         if str(err) == "Blacklisted key":
          self.print_stuff("Blacklisted key, using new key, dropping current.")
          drop = True

         if str(err) == "Service unavailable" and service != 10:
          self.print_stuff("Service unavailable, using new key")
          service += 1
         if str(err) == "Unauthorized" or "Internal server error":
          self.print_stuff("Unauthorized, using new key")
         if len(self.keydict)==1:
          self.print_stuff("Too many requests, not enough keys.")
          if hangwait == False:
           break 
          else:
           time.sleep(0.5)
         self.new_key(drop = drop)
       elif (str(err) == "Too many requests" and 'Retry-After' not in err.headers):
        time.sleep(0.01) 
       else:
        self.print_stuff("%s, Teams: %s" % (str(err), team_ids[(x*10):stop]), error= True)
        break
      except:
       self.print_stuff("Other Error Requesting Teams",error=True)
      else:
       finished = True

     for y in team_ids[(x*10):stop]:
 #      print y
      cur_team_full = todict(teams_data).get(y)
      cur_team = {}
      for n in ['createDate', 'fullId', 'lastGameDate', 'lastJoinDate', 'lastJoinedRankedTeamQueueDate', 'modifyDate', 'name', 'secondLastJoinDate', 'status', 'tag', 'thirdLastJoinDate']:
       try:
 #        print "tried"
        cur_team[n] = cur_team_full[n]

       except:
 #        print "set to null"
        cur_team[n] = None

 #      print cur_team      
 #      averageGamesPlayed3v3, losses3v3, wins3v3, averageGamesPlayed5v5, losses5v5, wins5v5

      try:
       if "teamStatDetails" in cur_team_full:
        team_stats =  cur_team_full['teamStatDetails']
     
        if team_stats[0]['teamStatType'] == "RANKED_TEAM_3x3":
         cur_team['averageGamesPlayed3v3'] = team_stats[0]['averageGamesPlayed']
         cur_team['losses3v3'] = team_stats[0]['losses']
         cur_team['wins3v3'] = team_stats[0]['wins']
         cur_team['averageGamesPlayed5v5'] = team_stats[1]['averageGamesPlayed']      
         cur_team['losses5v5'] = team_stats[1]['losses']
         cur_team['wins5v5'] = team_stats[1]['wins']
        elif team_stats[0]['teamStatType'] == "RANKED_TEAM_5x5":
         cur_team['averageGamesPlayed3v3'] = team_stats[1]['averageGamesPlayed']
         cur_team['losses3v3'] = team_stats[1]['losses']
         cur_team['wins3v3'] = team_stats[1]['wins']
         cur_team['averageGamesPlayed5v5'] = team_stats[0]['averageGamesPlayed']      
         cur_team['losses5v5'] = team_stats[0]['losses']
         cur_team['wins5v5'] = team_stats[0]['wins']
        else:
         self.print_stuff("Error, team stats messed up", error = True)
      
        try:
         self.cursor.execute(add_team, cur_team)
        except mysql.connector.Error as err:
         if err.errno != 1062 or self.suppress_duplicates == False:
          self.print_stuff("%s, Team: %s" % (err.errno, y), error= True)
   #       print add_team % cur_team
        else:
         self.print_stuff("Updated Team", override = True)

     
       all_teams = []
       team_history = True
       try:
        for n in cur_team_full['matchHistory']:
         cur_team_history = {}
         cur_team_history['fullId'] = y
         for z in ['assists', 'date', 'deaths', 'gameId', 'gameMode', 'invalid', 'kills', 'mapId', 'opposingTeamKills', 'opposingTeamName', 'win']:
          cur_team_history[z] = n[z]
         all_teams.append(cur_team_history)

     
  #        OLD METHOD
  #        try:
  # #         print(add_team_history, cur_team_history)
  #         self.cursor.execute(add_team_history, cur_team_history)
  #        
  #        except mysql.connector.Error as err:
  #         if err.errno != 1062:
  #          print "%s, Team: %s" (err.msg, y)
  #       
  #        else:
  #         print "Updated Team-History"
        
       except:
        team_history = False
        self.print_stuff( "No Team-History")


       if team_history == True:
        try:
         self.cursor.executemany(add_team_history, all_teams)
   #       print test_team
        except mysql.connector.Error as err:
         if err.errno != 1062 or self.suppress_duplicates == False:
          self.print_stuff("Error %s" % err.errno, error = True)
        else:
         self.print_stuff("Updated Team-History")
      
      
      

       all_teams = []
       team_roster = True
       try:  
        for n in cur_team_full['roster']['memberList']:
         cur_team_roster = {}
  #        print(n)
         for z in ['inviteDate', 'joinDate', 'playerId', 'status']:
  #         print n[z]
          try:
           cur_team_roster[z] = n[z]
          except:
           cur_team_roster[z] = None
         if cur_team_full['roster']['ownerId'] == n['playerId']:
          cur_team_roster['isCaptain'] = True
         else:
          cur_team_roster['isCaptain'] = False
         cur_team_roster['teamId'] = y
         all_teams.append(cur_team_roster)
       
  #      OLD METHOD
  #        try:
  #         self.cursor.execute(add_team_roster, cur_team_roster)
  #        except mysql.connector.Error as err:
  #         if err.errno != 1062:
  #          print "%s, Team: %s" % (err.msg, y)
  #        else:
  #         print "Updated Team-Roster"
       except:
        team_roster = False
        if (cur_team_full['status']=="DISBANDED"):
         self.print_stuff("No Team-Roster -- Team Disbanded")
        else:
         self.print_stuff("No Team-Roster")
     
     
       if team_roster == True:
        try:
         self.cursor.executemany(add_team_roster, all_teams)
   #       print test_team
        except mysql.connector.Error as err:
         if err.errno != 1062 or self.suppress_duplicates == False:
          self.print_stuff("Error %s" % err.errno, error = True)
        else:
         self.print_stuff("Updated Team-Roster")
     
      except:
       self.print_stuff("Error, no team information; %s" % (y), error=True)
    

     self.print_stuff("Finished %s of %s" % (stop, len(team_ids)), progress=True)
     if stop == len(team_ids):
      self.print_stuff("Finished %s of %s" % (stop, len(team_ids)),  header2=True)
    
     self.cnx.commit()
        
    if checkTeams==True:
     self.print_stuff("Checking Teams")
     self.update_table("checkteams", feedback=feedback, suppress_duplicates = self.suppress_duplicates, ignore_skiplist=ignore_skiplist, allow_updates=allow_updates)
    
  if table=="all":
   self.update_table("challenger", feedback=feedback, queue= queue, suppress_duplicates = self.suppress_duplicates)
   self.update_table("master", feedback=feedback, queue = queue, suppress_duplicates = self.suppress_duplicates)
   self.update_table("team", checkTeams =True, feedback=feedback, suppress_duplicates = self.suppress_duplicates, allow_updates = allow_updates, ignore_skiplist=ignore_skiplist)

  if table=="iterate":
     self.print_stuff("Iterating through possible summonerIds.", header1 = True)
     team_ids = []
     err = []
     for x in xrange(int(round(iteratestart, -1)/10), int((round(iteratestart+iterate, -1)/10))):
     
      stop = (((x+1)*10) if ((x+1)*10) <= int(iteratestart+iterate) else (int(iteratestart+iterate)))
      if x*10 == stop:
       continue

      ids = []
      for y in xrange((x*10), (x+1)*10):
       ids.append(y)
 #      print ids
     
      finished = False
      while finished == False:
       self.wait()
       try:
 #        print "trying"
        teams = self.w.get_teams_for_summoners(ids)
      
       except riotwatcher.riotwatcher.LoLException as err:
  #       print str(err)
        if str(err) == "Game data not found":
         finished = True
        elif (str(err) == "Too many requests" and 'Retry-After' in err.headers) or str(err) == "Unauthorized" or str(err) == "Blacklisted key":
 #         print "New Key" 
         drop = False
         if str(err) == "Blacklisted key":
          self.print_stuff("Blacklisted key, using new key, dropping current.")
          drop = True
         if str(err) == "Unauthorized":
          self.print_stuff("Unauthorized, using new key")
         if len(self.keydict)==1:
          self.print_stuff("Too many requests, not enough keys.", error=True)
          if hangwait == False:
           break 
          else:
           time.sleep(0.5)
         self.new_key(drop=drop)
        elif (str(err) == "Too many requests" and 'Retry-After' not in err.headers):
         time.sleep(0.01)
        else:

         self.print_stuff("%s, Team: %s" % (str(err), ids), error= True)
         break
       else:
        finished = True

      if err == "Game data not found":
        err = []
        continue
       
 #      print teams 
      for y in teams:
       for z in teams[y]:
        team_ids.append(z['fullId'])
 #         print v
      self.print_stuff("Finished %s of %s, %s teams found." % (int(stop-iteratestart), int(iterate), len(team_ids)), progress = True)
     self.print_stuff("Updating Team Table", header2 = True)
     self.update_table("team", teamIds = team_ids, checkTeams = checkTeams, feedback=feedback, suppress_duplicates = self.suppress_duplicates, allow_updates = allow_updates, ignore_skiplist=ignore_skiplist)  
 