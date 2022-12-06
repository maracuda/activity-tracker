select count(*)
from productivity.stats
where ActionType = 'MouseEvent'
    and splitByChar(':', visitParamExtractString(Value, 'data'))[1]='wheel'