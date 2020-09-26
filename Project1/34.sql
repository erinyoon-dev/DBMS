SELECT P.name, C.level, C.nickname
FROM Pokemon P, CatchedPokemon C, Gym G
WHERE C.owner_id = G.leader_id
  AND P.id = C.pid
  AND C.nickname LIKE 'A%'
ORDER BY P.name DESC;

