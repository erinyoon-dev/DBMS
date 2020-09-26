SELECT T.name AS 'leaderName', AVG(C.level) AS 'avgLevel'
FROM Gym G, Trainer T, CatchedPokemon C
WHERE G.leader_id = T.id
  AND T.id = C.owner_id
GROUP BY T.name
ORDER BY T.name;

