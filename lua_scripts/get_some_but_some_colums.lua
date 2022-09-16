---
--- Script to get some hashmaps of given keys but only get a handful of columns
--- Example usage:
---
--- EVAL "local result = {} local table_unpack = table.unpack or unpack local columns = {  } for i, k in ipairs(ARGV) do    if i > 1 then        table.insert(columns, k)    end end for _, key in ipairs(KEYS) do    local data = redis.call('HMGET', key, table_unpack(columns))    local parsed_data = {}    for i, v in ipairs(data) do        table.insert(parsed_data, columns[i])        table.insert(parsed_data, v)    end    table.insert(result, parsed_data) end return result" 2 "book_%&_Oliver Twist" "book_%&_Wuthering Heights" tags title rating author
---



local result = {}
local table_unpack = table.unpack or unpack
local columns = {  }

for i, k in ipairs(ARGV) do
    if i > 1 then
        table.insert(columns, k)
    end
end

for _, key in ipairs(KEYS) do
    local data = redis.call('HMGET', key, table_unpack(columns))
    local parsed_data = {}

    for i, v in ipairs(data) do
        table.insert(parsed_data, columns[i])
        table.insert(parsed_data, v)
    end

    table.insert(result, parsed_data)
end
return result