SELECT C.pid AS 'Pokemon_ID', P.name AS 'Name'
FROM Trainer T, Pokemon P, CatchedPokemon C
WHERE T.id = C.owner_id
  AND C.pid = P.id
  AND T.hometown='Sangnok City'
ORDER BY C.pid;

