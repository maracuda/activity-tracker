CREATE DATABASE productivity;

CREATE TABLE productivity.stats
(
    SessionId UUID,
    Timestamp DateTime,
    Username String,
    ActionType String,
    Value String
) ENGINE = MergeTree()
      Order BY Timestamp;

CREATE TABLE productivity.stats_named
(
    SessionId UUID,
    TaskName String,
    Timestamp DateTime,
    Username String,
    ActionType String,
    Value String
) ENGINE = MergeTree()
      Order BY Timestamp;
