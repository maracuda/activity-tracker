select right.x, right.y, left.x, left.y
from (
         select x, y, number
         from (
                  select splitByChar(':', visitParamExtractString(Value, 'data'))[2] x,
                         splitByChar(':', visitParamExtractString(Value, 'data'))[3] y,
                         Timestamp,
                         rowNumberInBlock()                                          number
                  from productivity.stats
                  where ActionType = 'MouseEvent'
                    and splitByChar(':', visitParamExtractString(Value, 'data'))[1] = 'move'
                  )
         order by Timestamp, number
         ) as left
         inner join
     (
         select x, y, number
         from (
                  select splitByChar(':', visitParamExtractString(Value, 'data'))[2] x,
                         splitByChar(':', visitParamExtractString(Value, 'data'))[3] y,
                         Timestamp,
                         rowNumberInBlock()                                          number
                  from productivity.stats
                  where ActionType = 'MouseEvent'
                    and splitByChar(':', visitParamExtractString(Value, 'data'))[1] = 'move'
                  )
         order by Timestamp, number
         ) as right
     on left.number = right.number + 1