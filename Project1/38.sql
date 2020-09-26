SELECT P.name
FROM Pokemon P, Evolution E
WHERE P.id = E.after_id
  AND E.after_id <> ALL (
    SELECT before_id
    FROM Evolution
    )
ORDER BY P.name;

