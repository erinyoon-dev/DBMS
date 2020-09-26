SELECT AVG(level) AS 'AverageLevel'
FROM CatchedPokemon C, Gym G
WHERE C.owner_id = G.leader_id;

