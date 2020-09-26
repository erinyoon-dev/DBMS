SELECT DISTINCT name
FROM Pokemon P
WHERE P.type = 'Grass'
  AND P.id = ANY (
    SELECT before_id
    FROM Pokemon P, Evolution E
    WHERE P.id = E.before_id
    );

