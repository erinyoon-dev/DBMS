SELECT A.name AS 'Trainer_Name', MAX(A.level) As 'MaxLevel'
FROM (
  SELECT T.name, C.level AS 'level'
  FROM Trainer T, CatchedPokemon C
  WHERE T.id = C.owner_id
  )A
GROUP BY A.name
HAVING COUNT(*)>=4;

