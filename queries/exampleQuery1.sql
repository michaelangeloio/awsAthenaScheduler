select 

'Baseball' as name, 
cast(count(baseball) as integer) as value 


from form_sport 

WHERE "baseball" = 'true' 


UNION 

select 

'Basketball' as name, 
cast(count(basketball) as integer) as value 


from form_sport 

WHERE "basketball" = 'true' 



UNION  

select  

'Football' as name, 
cast(count(football) as integer) as value 


from form_sport 

WHERE "football" = 'true' 

UNION  

select  

'Track & Field' as name, 
cast(count(track) as integer) as value 


from form_sport 

WHERE "track" = 'true' 

UNION  

select  

'Golf' as name, 
cast(count(golf) as integer) as value 


from form_sport 

WHERE "golf" = 'true' 


UNION  

select  

'Soccer (football)' as name, 
cast(count(soccer) as integer) as value 


from form_sport 

WHERE "soccer" = 'true' 


ORDER BY value DESC;