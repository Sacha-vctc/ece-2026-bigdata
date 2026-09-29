-- 1. Nombre moyen de commandes par utilisateur, et quantité moyenne par commande
SELECT
  round(count(*) / count(DISTINCT user_uuid), 2) AS avg_orders_per_user,
  round(avg(quantity), 2) AS avg_quantity_per_order
FROM orders;

-- 2. Pour chaque utilisateur : première et dernière commande, et nombre de jours entre les deux
SELECT
  u.username,
  min(o.date) AS first_order,
  max(o.date) AS last_order,
  date_diff('day', min(o.date), max(o.date)) AS days_between
FROM orders o
JOIN users u ON o.user_uuid = u.uuid
GROUP BY ALL
ORDER BY days_between DESC;

-- 3. Heure de la journée avec la plus grande quantité vendue
SELECT hour(date) AS hour, sum(quantity) AS quantity
FROM orders
GROUP BY hour
ORDER BY quantity DESC
LIMIT 1;

-- 4. Variation mensuelle (en %) de la quantité vendue par produit
WITH monthly AS (
  SELECT product, date_trunc('month', date) AS month, sum(quantity) AS quantity
  FROM orders
  GROUP BY ALL
),
with_previous AS (
  SELECT
    product,
    month,
    quantity,
    lag(quantity) OVER (PARTITION BY product ORDER BY month) AS previous_quantity
  FROM monthly
)
SELECT
  product,
  strftime(month, '%Y-%m') AS month,
  quantity,
  previous_quantity,
  round(100.0 * (quantity - previous_quantity) / previous_quantity, 1) AS variation_pct
FROM with_previous
ORDER BY product, month;

-- 5. Utilisateurs ayant commandé chaque produit au moins une fois
SELECT u.username, u.name, count(DISTINCT o.product) AS products
FROM orders o
JOIN users u ON o.user_uuid = u.uuid
GROUP BY ALL
HAVING count(DISTINCT o.product) = (SELECT count(DISTINCT product) FROM orders)
ORDER BY u.username;
