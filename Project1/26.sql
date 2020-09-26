SELECT name
FROM Pokemon P, CatchedPokemon C
WHERE P.id = C.pid
  AND C.nickname LIKE '% %'
ORDER BY name DESC;

