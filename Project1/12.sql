SELECT DISTINCT name, type
FROM Pokemon P, CatchedPokemon C
WHERE P.id = C.pid
  AND C.level>=30
ORDER BY name;

