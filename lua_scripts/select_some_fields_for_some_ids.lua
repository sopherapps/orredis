---
--- Script to get some hashmaps of given keys but only get a handful of columns, with nested columns specified by repetition
--- Example usage:
---
--- EVAL "local result = {} local table_unpack = table.unpack or unpack local columns = { } local nested_columns = {} local args_tracker = {} for i, k in ipairs(ARGV) do if args_tracker[k] then nested_columns[k] = true else table.insert(columns, k) args_tracker[k] = true end end for _, key in ipairs(KEYS) do local data = redis.call('HMGET', key, table_unpack(columns)) local parsed_data = {} for i, v in ipairs(data) do if v then table.insert(parsed_data, columns[i]) if nested_columns[columns[i]] then v = redis.call('HGETALL', v) end table.insert(parsed_data, v) end end table.insert(result, parsed_data) end return result" 2 "book_%&_Oliver Twist" "book_%&_Wuthering Heights" tags title rating author author
---


local result = {}
local table_unpack = table.unpack or unpack
local columns = {  }
local nested_columns = {}
local args_tracker = {}

for i, k in ipairs(ARGV) do
    if args_tracker[k] then
        nested_columns[k] = true
    else
        table.insert(columns, k)
        args_tracker[k] = true
    end
end

for _, key in ipairs(KEYS) do
    local data = redis.call('HMGET', key, table_unpack(columns))
    local parsed_data = {}

    for i, v in ipairs(data) do
        if v then
            table.insert(parsed_data, columns[i])

            if nested_columns[columns[i]] then
                v = redis.call('HGETALL', v)
            end

            table.insert(parsed_data, v)
        end
    end

    table.insert(result, parsed_data)
end
return result