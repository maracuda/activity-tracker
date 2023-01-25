select sessions.SessionId           as SessionId,
       sessions.username            as Login,
       hotkeys_2.navigationsCount   as NavigationsCount,
       mouseClicks.mouseClicksCount as MouseClicks,
       mouseWheels.mouseWheelCount  as MouseWheels,
       mouseMoves.distance          as MouseDistance,
       hotkeys_2.navigationsCount+mouseClicks.mouseClicksCount+mouseWheels.mouseWheelCount +mouseMoves.distance as Total
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

        -- стрелки
        left join
    (select SessionId,
            count(visitParamExtractString(Value, 'data')) navigationsCount
     from productivity.stats
     where ActionType = 'Action'
       and visitParamExtractString(Value, 'data') in
           ('EditorDown',
            'EditorUp',
            'EditorLeft',
            'EditorRight',
            'EditorPreviousWord',
            'EditorNextWord',
            'EditorLineEnd')
     group by SessionId
        ) hotkeys_2
    on sessions.SessionId = hotkeys_2.SessionId

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
order by sessions.start desc;
