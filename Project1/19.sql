SELECT COUNT(*) AS 'Sangnok_#PokemonType'
FROM (
  SELECT P.type, count(*)
  FROM Pokemon P, CatchedPokemon C, Gym G
  WHERE P.id = C.pid
    AND G.leader_id = C.owner_id
    AND G.city = 'Sangnok City'
  GROUP BY P.type
  )Type;
