select min(Username), SessionId, visitParamExtractString(Value, 'data') HotkeyName, count(*) Count
from productivity.stats
where ActionType = 'Action'
group by SessionId, visitParamExtractString(Value, 'data')
order by Count desc
