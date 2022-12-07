select sessions.SessionId,
       sessions.username,
       sessions.start,
       sessions.duration,
       hotkeys.hotkeyCount,
       mouseClicks.mouseClicksCount,
       mouseMoves.distance
from (select SessionId,
             min(Timestamp)                                     start,
             max(Timestamp)                                     end,
             dateDiff('minute', min(Timestamp), max(Timestamp)) duration,
             min(Username)                                      username
      from productivity.stats
      group by SessionId
      order by start desc
         ) sessions
         join
     (select SessionId, count(*) hotkeyCount
      from productivity.stats
      where ActionType = 'Action'
      group by SessionId
         ) hotkeys
     on sessions.SessionId = hotkeys.SessionId
         join
     (select SessionId, count(*) mouseClicksCount
      from productivity.stats
      where ActionType = 'MouseEvent'
        and splitByChar(':', visitParamExtractString(Value, 'data'))[1] = 'click'
      group by SessionId) mouseClicks
     on sessions.SessionId = mouseClicks.SessionId
         join
     (select SessionId,
             sum(L2Distance((toFloat32(right.x), toFloat32(right.y)), (toFloat32(left.x), toFloat32(left.y)))) distance
      from (
               select x, y, number, SessionId
               from (
                        select splitByChar(':', visitParamExtractString(Value, 'data'))[2] x,
                               splitByChar(':', visitParamExtractString(Value, 'data'))[3] y,
                               Timestamp,
                               rowNumberInBlock()                                          number,
                               SessionId
                        from productivity.stats
                        where ActionType = 'MouseEvent'
                          and splitByChar(':', visitParamExtractString(Value, 'data'))[1] = 'move'
                        )
               order by Timestamp, number
               ) as left
               inner join
           (
               select x, y, number, SessionId
               from (
                        select splitByChar(':', visitParamExtractString(Value, 'data'))[2] x,
                               splitByChar(':', visitParamExtractString(Value, 'data'))[3] y,
                               Timestamp,
                               rowNumberInBlock()                                          number,
                               SessionId
                        from productivity.stats
                        where ActionType = 'MouseEvent'
                          and splitByChar(':', visitParamExtractString(Value, 'data'))[1] = 'move'
                        )
               order by Timestamp, number
               ) as right
           on left.SessionId = right.SessionId and left.number = right.number + 1
      group by SessionId) mouseMoves
     on mouseMoves.SessionId = sessions.SessionId