SELECT AVG(C.level) AS 'Red\'s Pokemon_AvgLevel'
FROM CatchedPokemon C, Trainer T
WHERE C.owner_id = T.id
  AND T.name='Red';

