SELECT AVG(C.level) AS 'Sangnok_Electric_AvgLevel'
FROM Trainer T, CatchedPokemon C, Pokemon P
WHERE T.id = C.owner_id
  AND T.hometown = 'Sangnok City'
  AND C.pid = P.id
  AND P.type = 'Electric';

