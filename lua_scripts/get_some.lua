---
--- Script to get some ids hashmaps
--- Example usage:
---
--- EVAL "local result = {} for _, key in ipairs(KEYS) do    table.insert(result, redis.call('HGETALL', key)) end return result" 2 "book_%&_Oliver Twist" "book_%&_Wuthering Heights"
---


local result = {}
for _, key in ipairs(KEYS) do
    table.insert(result, redis.call('HGETALL', key))
end
return result