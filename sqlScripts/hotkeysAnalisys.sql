select visitParamExtractString(Value, 'data') HotkeyName, count(*) count, startsWith(HotkeyName, 'Editor')
from productivity.stats
where ActionType = 'Action'
    and SessionId = '9797552c-db48-4026-a2ea-4f28b268f52f'
group by HotkeyName
order by count desc