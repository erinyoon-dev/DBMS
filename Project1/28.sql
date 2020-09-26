SELECT A.name AS 'Trainer_Name', ROUND(AVG(A.level)) As 'AvgLevel'
FROM (
  SELECT T.name, C.level AS 'level'
  FROM Trainer T, CatchedPokemon C, Pokemon P
  WHERE T.id = C.owner_id
    AND C.pid = P.id
    AND P.type IN ('Normal', 'Electric')
  )A
GROUP BY Trainer_Name
ORDER BY AvgLevel;

