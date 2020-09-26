SELECT P.type, COUNT(*) AS '#Pokemon'
FROM CatchedPokemon C, Pokemon P
WHERE C.pid = P.id
GROUP BY P.type
ORDER BY P.type;

