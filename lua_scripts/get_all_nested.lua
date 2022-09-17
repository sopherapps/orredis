---
--- Script to get all hashmaps that have a given pattern, and include their nested data, to only one level of nesting
--- Example usage:
---
--- EVAL "local filtered = {} local cursor = '0' local nested_fields = {} for i, key in ipairs(ARGV) do if i > 1 then nested_fields[key] = true end end repeat local result = redis.call('SCAN', cursor, 'MATCH', ARGV[1]) for _, key in ipairs(result[2]) do if redis.call('TYPE', key).ok == 'hash' then local parent = redis.call('HGETALL', key) for i, k in ipairs(parent) do if nested_fields[k] then local nested = redis.call('HGETALL', parent[i + 1]) parent[i + 1] = nested end end table.insert(filtered, parent) end end cursor = result[1] until (cursor == '0') return filtered" 0 "book_*" author
---

local filtered = {}
local cursor = '0'
local nested_fields = {}

for i, key in ipairs(ARGV) do
    if i > 1 then
        nested_fields[key] = true
    end
end

repeat
    local result = redis.call('SCAN', cursor, 'MATCH', ARGV[1])
    for _, key in ipairs(result[2]) do
        if redis.call('TYPE', key).ok == 'hash' then
            local parent = redis.call('HGETALL', key)

            for i, k in ipairs(parent) do
                if nested_fields[k] then
                    local nested = redis.call('HGETALL', parent[i + 1])
                    parent[i + 1] = nested
                end
            end

            table.insert(filtered, parent)
        end
    end
    cursor = result[1]
until (cursor == '0')
return filtered