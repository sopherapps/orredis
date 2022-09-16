---
--- Script to get all hashmaps that have a given pattern, and include their nested data, to only one level of nesting
--- Example usage:
---
--- EVAL "local filtered = {} local cursor = '0' local table_unpack = table.unpack or unpack local columns = {  } local nested_columns = {} local args_tracker = {} for i, k in ipairs(ARGV) do    if i > 1 then        if args_tracker[k] then            nested_columns[k] = true        else            table.insert(columns, k)            args_tracker[k] = true        end    end end repeat    local result = redis.call('SCAN', cursor, 'MATCH', ARGV[1])    for _, key in ipairs(result[2]) do        if redis.call('TYPE', key).ok == 'hash' then            local data = redis.call('HMGET', key, table_unpack(columns))            local parsed_data = {}            for i, v in ipairs(data) do                table.insert(parsed_data, columns[i])                                if nested_columns[columns[i]] then                    v = redis.call('HGETALL', v)                end                table.insert(parsed_data, v)            end            table.insert(filtered, parsed_data)        end    end    cursor = result[1] until (cursor == '0') return filtered" 0 "book_\%\&_*" tags title rating author author
---

local filtered = {}
local cursor = '0'
local nested_fields = {}

for _, key in ipairs(ARGV) do
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
                    table.insert(parent, i + 1, nested)
                end
            end

            table.insert(filtered, parent)
        end
    end
    cursor = result[1]
until (cursor == '0')
return filtered