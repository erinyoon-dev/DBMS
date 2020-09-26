SELECT SUM(C.level) AS 'Matis\'s_Total_Lv'
FROM CatchedPokemon C, Trainer T
WHERE C.owner_id = T.id
  AND T.name = 'Matis';

