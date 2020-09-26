SELECT COUNT(*) AS 'TotalCount'
FROM Pokemon P
WHERE P.type = 'Water'
  OR P.type = 'Electric'
  OR P.type = 'Psychic';

