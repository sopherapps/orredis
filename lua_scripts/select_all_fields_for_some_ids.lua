---
--- Script to get some ids hashmaps with nested fields
--- Example usage:
---
--- EVAL "local result = {} local nested_fields = {} for _, key in ipairs(ARGV) do nested_fields[key] = true end for _, key in ipairs(KEYS) do local parent = redis.call('HGETALL', key) for i, k in ipairs(parent) do if nested_fields[k] then local nested = redis.call('HGETALL', parent[i + 1]) parent[i + 1] = nested end end table.insert(result, parent) end return result" 2 "book_%&_Oliver Twist" "book_%&_Wuthering Heights" author
---


local result = {}
local nested_fields = {}

for _, key in ipairs(ARGV) do
    nested_fields[key] = true
end

for _, key in ipairs(KEYS) do
    local parent = redis.call('HGETALL', key)

    for i, k in ipairs(parent) do
        if nested_fields[k] then
            local nested = redis.call('HGETALL', parent[i + 1])
            parent[i + 1] = nested
        end
    end

    table.insert(result, parent)
end
return result