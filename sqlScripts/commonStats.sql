select sessions.SessionId,
       sessions.username,
       sessions.start,
       sessions.duration,
       hotkeys.hotkeyCount,
       keyboard.keysCount,
       mouseClicks.mouseClicksCount,
       mouseWheels.mouseWheelCount,
       mouseMoves.distance
from
    -- Сессии
    (select SessionId,
            min(Timestamp)                                     start,
            max(Timestamp)                                     end,
            dateDiff('minute', min(Timestamp), max(Timestamp)) duration,
            min(Username)                                      username
     from productivity.stats
     group by SessionId
     order by start desc
        ) sessions

        -- хоткеи
        left join
    (select SessionId, count(*) hotkeyCount
     from productivity.stats
     where ActionType = 'Action'
     group by SessionId
        ) hotkeys
    on sessions.SessionId = hotkeys.SessionId

        -- мышка клики
        left join
    (select SessionId,
            count(*) mouseClicksCount
     from productivity.stats
     where ActionType = 'MouseEvent'
       and splitByChar(':', visitParamExtractString(Value, 'data'))[1] = 'click'
     group by SessionId) mouseClicks
    on sessions.SessionId = mouseClicks.SessionId

        -- мышка колесо
        left join
    (select SessionId,
            count(*) mouseWheelCount
     from productivity.stats
     where ActionType = 'MouseEvent'
       and splitByChar(':', visitParamExtractString(Value, 'data'))[1] = 'wheel'
     group by SessionId) mouseWheels
    on sessions.SessionId = mouseWheels.SessionId

        -- клавиши
        join
    (select SessionId, count(*) keysCount
     from productivity.stats
     where ActionType = 'KeyEvent'
     group by SessionId) keyboard
    on keyboard.SessionId = sessions.SessionId

        -- пробег мышки
        left join
    (select SessionId,
            sum(L2Distance((toFloat32(right.x), toFloat32(right.y)),
                           (toFloat32(left.x), toFloat32(left.y)))) distance
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
order by sessions.start desc