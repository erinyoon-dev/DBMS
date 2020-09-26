SELECT T.hometown AS 'Hometown', ROUND(AVG(C.level), 0) AS 'AvgLevel'
FROM Trainer T, CatchedPokemon C
WHERE T.id = C.owner_id
GROUP BY T.hometown
ORDER BY AVG(C.level);

