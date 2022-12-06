select SessionId, count(*)
from productivity.stats
where ActionType = 'KeyEvent'
group by SessionId