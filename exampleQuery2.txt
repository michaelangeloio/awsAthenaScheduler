select 

food_response_num as name, 
cast(count(food_response_num) as integer) as value 

from form_who 
GROUP BY food_response_num 

HAVING "food_response_num" IN ('Visitor', 'Friend') 
ORDER BY value DESC;