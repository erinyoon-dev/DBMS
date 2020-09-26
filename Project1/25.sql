SELECT DISTINCT name AS 'Common_Pokemon'
FROM Pokemon P, (
  SELECT C.pid AS 'pid'
  FROM CatchedPokemon C, Trainer T
  WHERE C.owner_id = T.id
    AND T.hometown = 'Sangnok City'
  )A INNER JOIN (SELECT C.pid AS 'pid'
                 FROM CatchedPokemon C, Trainer T
                 WHERE C.owner_id = T.id
                 AND T.hometown = 'Brown City')B
WHERE P.id = A.pid
  AND P.id = B.pid
ORDER BY P.name;

