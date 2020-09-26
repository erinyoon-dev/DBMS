SELECT DISTINCT T.name AS 'Trainer_Name'
FROM Trainer T, CatchedPokemon C
WHERE T.id = C.owner_id
  AND C.level<=10
ORDER BY T.name;

