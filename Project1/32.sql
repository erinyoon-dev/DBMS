SELECT name
FROM Pokemon P
WHERE P.id <> ALL (
  SELECT pid
  FROM CatchedPokemon C, Pokemon P
  WHERE C.pid = P.id
  )
ORDER BY name;

