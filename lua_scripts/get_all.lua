---
--- Script to get all hashmaps that have a given pattern
--- Example usage:
---
--- EVAL "local filtered = {} local cursor = '0' repeat    local result = redis.call('SCAN', cursor, 'MATCH', ARGV[1])    for _, key in ipairs(result[2]) do        if redis.call('TYPE', key).ok == 'hash' then            table.insert(filtered, redis.call('HGETALL', key))        end    end    cursor = result[1] until (cursor == '0') return filtered" 0 "author_\%\&_*"
---

local filtered = {}
local cursor = '0'
repeat
    local result = redis.call('SCAN', cursor, 'MATCH', ARGV[1])
    for _, key in ipairs(result[2]) do
        if redis.call('TYPE', key).ok == 'hash' then
            table.insert(filtered, redis.call('HGETALL', key))
        end
    end
    cursor = result[1]
until (cursor == '0')
return filtered