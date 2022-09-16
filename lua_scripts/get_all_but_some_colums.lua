---
--- Script to get all hashmaps that have a given pattern but only get a handful of columns
--- Example usage:
---
--- EVAL "local filtered = {} local cursor = '0' local table_unpack = table.unpack or unpack local columns = {  } for i, k in ipairs(ARGV) do    if i > 1 then        table.insert(columns, k)    end end repeat    local result = redis.call('SCAN', cursor, 'MATCH', ARGV[1])    for _, key in ipairs(result[2]) do        if redis.call('TYPE', key).ok == 'hash' then            local data = redis.call('HMGET', key, table_unpack(columns))            local parsed_data = {}            for i, v in ipairs(data) do                table.insert(parsed_data, columns[i])                table.insert(parsed_data, v)            end            table.insert(filtered, parsed_data)        end    end    cursor = result[1] until (cursor == '0') return filtered" 0 "book_\%\&_*" tags title rating author
---


local filtered = {}
local cursor = '0'
local table_unpack = table.unpack or unpack
local columns = {  }

for i, k in ipairs(ARGV) do
    if i > 1 then
        table.insert(columns, k)
    end
end

repeat
    local result = redis.call('SCAN', cursor, 'MATCH', ARGV[1])
    for _, key in ipairs(result[2]) do
        if redis.call('TYPE', key).ok == 'hash' then
            local data = redis.call('HMGET', key, table_unpack(columns))
            local parsed_data = {}

            for i, v in ipairs(data) do
                table.insert(parsed_data, columns[i])
                table.insert(parsed_data, v)
            end

            table.insert(filtered, parsed_data)
        end
    end
    cursor = result[1]
until (cursor == '0')
return filtered