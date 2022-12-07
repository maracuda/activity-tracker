select SessionId, sum(L2Distance((toFloat32(right.x), toFloat32(right.y)), (toFloat32(left.x), toFloat32(left.y))))
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
group by SessionId