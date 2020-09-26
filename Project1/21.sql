SELECT T.name AS 'Leader_Name', COUNT(*) AS '#Pokemon'
FROM Gym G, CatchedPokemon C, Trainer T
WHERE G.leader_id = T.id
  AND T.id = C.owner_id
GROUP BY T.name
ORDER BY T.name;

