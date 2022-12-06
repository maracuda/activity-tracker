select visitParamExtractString(Value, 'data'), count(*)
from productivity.stats
where ActionType = 'Action'
group by visitParamExtractString(Value, 'data')