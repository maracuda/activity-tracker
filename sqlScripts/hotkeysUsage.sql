select visitParamExtractString(Value, 'data') HotkeyName, count(*) Count
from productivity.stats
where ActionType = 'Action'
group by visitParamExtractString(Value, 'data')
order by Count desc