# Labs Spark : RDD (module 06) et DataFrames (module 07)

| Fichier | Contenu |
|---|---|
| `lab06_rdd.py` | Lab « Introduction to Spark's RDD API » et ses 4 exercices |
| `lab07_silver.py` | Lab « Bronze to silver with Spark DataFrames » et ses 4 exercices |

```bash
# Lab 06 : depuis la racine du projet, avec users.csv et orders.csv téléchargés depuis bronze/
spark-submit spark_labs/lab06_rdd.py

# Lab 07 : lit et écrit directement dans le bucket S3
spark-submit --packages "$ICEBERG_PACKAGE" spark_labs/lab07_silver.py
```

Données : 50 utilisateurs et 2 829 commandes générés par `dataset-users` et `dataset-orders`
(le sujet en mentionne 5 000, notre générateur en produit 2 829).

---

## Lab 06 : RDD

### Création et partitions

**Qu'est-ce qui décide du nombre de partitions de `textFile()` pour un fichier de cette taille ?**
`textFile()` découpe le fichier en *splits* Hadoop d'au plus la taille de bloc (32 à 128 Mo), avec un minimum de
`sc.defaultMinPartitions = min(defaultParallelism, 2)`. Nos fichiers font 7 Ko et 330 Ko, bien moins qu'un bloc :
c'est donc ce minimum qui s'applique, d'où **2 partitions**.

**`repartition()` ou `coalesce()` : lequel provoque un shuffle ?**
`repartition(n)` fait toujours un shuffle complet : les lignes sont redistribuées sur `n` partitions de tailles
équilibrées, et c'est la seule façon d'*augmenter* le nombre de partitions. `coalesce(n)` sert à le *réduire* sans
shuffle, en fusionnant des partitions voisines sur place. C'est moins cher, mais les partitions obtenues peuvent
être déséquilibrées.

### Évaluation paresseuse

**Au second `count()`, l'UI montre-t-elle un nouveau stage ? Pourquoi ?**
Oui. Un RDD n'est qu'une **lignée** de transformations, pas un résultat stocké. Chaque action relance tout le
calcul depuis le fichier : relecture, puis filtrage.

**À quel endroit `.cache()` changerait-il cette réponse ?**
En appelant `orders_data.cache()` (ou `persist()`) **avant la première action**. Le premier `count()` matérialise
alors les partitions en mémoire, et le second les relit depuis le cache, sans relire le fichier. Dans l'UI, le
stage apparaît encore, mais ses tâches sont bien plus rapides et l'onglet **Storage** montre le RDD en cache.

### Adresse sur plusieurs lignes

`users_lines.count()` = **101** lignes physiques (1 en-tête + 50 utilisateurs × 2 lignes), contre **50** enregistrements
après le filtre sur l'UUID.

**Pourquoi `spark.read.csv(multiLine=True)` n'a-t-il pas ce problème ?**
Le lecteur CSV comprend la syntaxe CSV : un saut de ligne situé **entre guillemets** fait partie du champ.
`textFile()` ne connaît que les octets et coupe à chaque `\n`, sans aucune notion de guillemet ni de colonne.
`multiLine=True` a un coût : le fichier ne peut plus être découpé en plusieurs partitions, puisqu'une coupure
pourrait tomber au milieu d'un enregistrement.

**Quelle condition un format doit-il remplir pour être lu sans risque ligne par ligne ?**
Chaque enregistrement doit tenir sur **exactement une ligne** : aucun saut de ligne dans les valeurs, ou alors
échappé (`\n` écrit comme deux caractères). C'est le cas du JSON Lines, du TSV ou des journaux d'application.
Le format doit aussi permettre de reconnaître le début d'un enregistrement depuis n'importe quel point du
fichier, pour pouvoir le découper.

### Parsing manuel

**Que se passe-t-il si une ligne a une virgule en trop dans `product` ?**
`line.split(",")` renvoie 6 éléments au lieu de 5 : `ValueError: too many values to unpack`. Rien ne se passe à la
définition de `map()`. L'erreur éclate **à la première action** qui touche cette ligne, et elle fait échouer tout le
job. Le script le démontre sur une ligne `...,choco,late`.

**Où le DataFrame API fait-il la vérification équivalente, et quand ?**
Dans le lecteur CSV, qui gère les guillemets et applique le **schéma** (inféré ou déclaré) au moment de la lecture,
c'est-à-dire à la première action. Selon le `mode`, une ligne invalide est mise à `NULL` (`PERMISSIVE`, défaut),
ignorée (`DROPMALFORMED`) ou fait échouer la lecture (`FAILFAST`). Les noms de colonnes et les types sont en plus
vérifiés **avant** toute exécution, à l'analyse du plan : une colonne inexistante est rejetée immédiatement.

### `reduceByKey` contre `groupByKey`

**Que shuffle `groupByKey`, et pourquoi est-ce plus cher ?**
`reduceByKey` additionne d'abord les valeurs **dans chaque partition** (*map-side combine*) : il n'envoie sur le
réseau qu'une paire `(user_id, somme partielle)` par utilisateur et par partition, soit au plus 50 × 2 ici.
`groupByKey` envoie **toutes les paires** `(user_id, quantity)`, soit 2 829 ici, puis construit en mémoire la liste
complète des valeurs de chaque clé. C'est plus de données sérialisées et transférées, et un risque de manque de
mémoire si une clé a beaucoup de valeurs. On le voit dans l'UI dans les colonnes *Shuffle Read/Write*.

**Pour quelles agrégations `groupByKey` est-il inévitable ?**
Quand on a besoin de **toutes les valeurs ensemble**, et que le résultat ne peut pas être calculé à partir de
résultats partiels combinés deux à deux : la médiane, un percentile exact, trier les commandes d'un utilisateur pour
calculer l'intervalle entre deux commandes, ou construire la liste complète des produits commandés. `reduceByKey`
exige une fonction associative et commutative.

### Déduplication manuelle

**Qu'est-ce qui joue ici le rôle du `partitionBy()` de la fenêtre ?**
Le **shuffle de `reduceByKey`**, sur la clé `order_id` produite par `map(lambda o: (o["order_id"], o))`. Il regroupe
toutes les lignes d'un même `order_id` dans une même partition, avant d'appliquer `most_recent`. On le voit dans la
lignée sous la forme d'un `ShuffledRDD ... at partitionBy`.

**Quelle version exprime le plus directement l'intention ?**
La **fonction de fenêtre** : « numéroter les lignes de chaque commande de la plus récente à la plus ancienne et
garder la première » se lit comme la règle métier. La version `reduceByKey` oblige à écrire un combinateur à la main,
et elle a un piège : en cas d'égalité de date, le résultat dépend de l'ordre d'arrivée des lignes. La fenêtre est en
plus optimisée par Catalyst (`WindowGroupLimit`).

### Jointure

**Comment Spark pourrait-il éviter le shuffle d'un `join()` ?**
Il faudrait que les deux RDD soient **déjà partitionnés de la même façon** sur la clé : même `Partitioner`, même
nombre de partitions, par exemple après `partitionBy(n)` sur les deux, puis mis en cache pour conserver ce
partitionnement. Les lignes d'un même `user_id` seraient alors déjà dans la même partition des deux côtés, et la
jointure deviendrait une dépendance étroite, sans shuffle. Autre option quand un côté est petit, comme les 50
utilisateurs : diffuser ce côté à chaque exécuteur (*broadcast join*).

**Que change `leftOuterJoin()` pour un `user_id` sans utilisateur ?**
Avec `join()`, la commande **disparaît** du résultat. Avec `leftOuterJoin()`, elle est **conservée**, sous la forme
`(user_id, (order, None))`. Ici, les deux comptent 2 829 lignes, puisque toutes les commandes ont un utilisateur.
L'exercice 2 injecte un faux `user_id` et retrouve exactement **1** commande orpheline.

**Quel est le risque d'un `.collect()` sur `enriched` avec beaucoup plus de données ?**
`collect()` rapatrie **toutes** les lignes dans la mémoire du **driver**, une seule JVM. Sur un gros volume, c'est
un `OutOfMemoryError` qui fait tomber l'application, et un transfert réseau énorme. Une jointure peut en plus
*multiplier* les lignes. On filtre et on agrège d'abord, puis on utilise `take(n)`, `count()`, ou on écrit le
résultat dans des fichiers.

### Exercices du lab 06

1. **`aggregateByKey`** : un seul passage calcule `(nombre_de_commandes, quantité_totale)` par utilisateur avec la
   valeur initiale `(0, 0)`, une fonction intra-partition et une fonction de fusion entre partitions. On en déduit
   la moyenne. Le script vérifie que les totaux sont identiques à ceux de `reduceByKey`.
2. **Faux `user_id` + `leftOuterJoin()`** : on filtre les lignes dont le nom d'utilisateur vaut `None` et on trouve
   **1** commande orpheline.
3. **DataFrame API** : `groupBy("user_uuid").sum("quantity")` tient en **une ligne**, contre 3 à 4 avec les RDD, en
   comptant le parsing manuel. `.explain()` montre un **plan physique optimisé** : agrégation partielle
   (`partial_sum`), puis `Exchange hashpartitioning` (le shuffle), puis agrégation finale. Le `FileScan` ne lit que
   les **2 colonnes utiles** (`ReadSchema: struct<user_uuid,quantity>`). `.toDebugString()` montre seulement la
   **lignée** des RDD (`HadoopRDD` → `PythonRDD` → `ShuffledRDD`) : Spark exécute les lambdas Python sans les
   comprendre, donc il ne peut ni élaguer de colonnes ni réordonner les opérations.
4. **`.cache()`** : avec le cache, le second `count()` est plus rapide et l'onglet **Storage** de l'UI liste le RDD
   (environ 170 Kio en mémoire). Sans cache, chaque action relit et reparse le fichier, et l'onglet Storage est vide.
   Sur un fichier aussi petit, l'écart reste faible, de l'ordre de la dizaine de millisecondes : le coût fixe de
   lancement des tâches domine.

---

## Lab 07 : DataFrames et Spark SQL

### SparkSession avec accès S3

**Que devient un job en cours si le service redémarre ?**
Le job **meurt** : en mode `local[*]`, le driver et les exécuteurs vivent dans le conteneur du service, et tout ce
qui n'est pas écrit sur S3 (cache, résultats intermédiaires) est perdu. Le nouveau service reçoit en plus de
**nouveaux identifiants STS** : les anciens, figés dans la configuration de la session, expirent, et toute requête
S3 échoue avec `403` ou `ExpiredToken`. Il faut recréer la session. Une écriture interrompue peut laisser un
répertoire Parquet à moitié écrit, ce qu'Iceberg évite.

**Pourquoi `SimpleAWSCredentialsProvider` échoue-t-il ici ?**
Il n'utilise qu'une clé d'accès et une clé secrète, et **ignore le jeton de session**. Or des identifiants STS
temporaires ne sont valides qu'accompagnés de leur `AWS_SESSION_TOKEN` : sans lui, le serveur refuse la signature.
`TemporaryAWSCredentialsProvider` envoie les trois. Avec des clés permanentes (un utilisateur IAM classique), il
n'y a pas de jeton, donc le fournisseur simple suffisait.

### Silver layer

**Expliquer le résultat de la requête par État.**
Elle compte les utilisateurs par code d'État et, parmi eux, ceux sans ville. Les codes militaires `AE`, `AP` et `AA`
ont **autant de villes manquantes que d'utilisateurs** : leurs adresses `DPO AE 12345` ou `FPO AP 09617` n'ont pas
de ville, et la regex de `city` exige une virgule. Tous les autres États ont `missing_city = 0`, ce qui confirme que
le découpage d'adresse fonctionne. Les effectifs sont petits (2 ou 3 par État), puisque 50 utilisateurs sont
répartis aléatoirement sur une soixantaine de codes.

**DataFrame ou SQL : lequel pour une exploration ponctuelle, lequel pour un modèle partagé ?**
Les deux compilent vers le **même plan Catalyst** : le choix porte sur la lisibilité et la maintenance, pas sur la
performance. Pour une **exploration ponctuelle**, le **SQL** est plus rapide à écrire et lisible par tous. Pour un
**modèle réutilisé**, l'**API DataFrame** permet de factoriser les transformations en fonctions Python testables
unitairement, de les composer, de paramétrer les colonnes, et donne des erreurs détectées plus tôt par l'IDE.
Une équipe d'analystes SQL préférera tout de même le SQL, ce que dbt formalise.

**Où ailleurs le `""` de `regexp_extract()` pourrait-il fausser un résultat ?**
Pour **`state`** et **`zip_code`**, qui ne sont pas convertis en `NULL`. Une adresse qui ne correspond pas à la regex
produirait `state = ""`, compté comme un « État » à part dans le `GROUP BY state`, et non signalé par un test
`not_null`. De même, un `zip_code` vide fausserait un test de cohérence code postal / État, ou ferait échouer un
`cast(zip_code as int)`. Il faut appliquer le même `when(col == "", None)` ou utiliser `nullif`.

### Écriture en Parquet

**Combien de part-files sous `users.parquet/`, et pourquoi ?**
Un fichier `part-00000-*.snappy.parquet` par **partition du DataFrame au moment de l'écriture**, plus un marqueur
`_SUCCESS`. Ici, il y en a **1** : le CSV de 7 Ko n'occupe qu'une partition. Pour `orders`, Spark coalesce
automatiquement (AQE) les 200 partitions issues du shuffle de la fenêtre, et on obtient aussi un seul fichier vu le
petit volume.

**Que perd-on avec `coalesce(1)` sur un gros jeu de données ?**
Le **parallélisme** : une seule tâche, sur un seul cœur, écrit tout le fichier pendant que les autres exécuteurs
attendent. Cette tâche doit aussi recevoir toutes les données, au risque de manquer de mémoire. À la lecture, un
seul gros fichier se parallélise moins bien que plusieurs fichiers de 128 Mo à 1 Go.

### Écriture en Iceberg

**`silver/spark/orders.parquet/` contre `warehouse/silver/orders/` : quels fichiers contiennent les données, et
lesquels décrivent la table ?**
Dans les deux cas, les **données** sont des fichiers Parquet : les `part-*.parquet` d'un côté, `data/*.parquet` de
l'autre. Le répertoire Parquet n'a rien d'autre qu'un marqueur `_SUCCESS`. Iceberg ajoute un répertoire
`metadata/` qui **décrit la table** : `vN.metadata.json` (schéma, partitionnement, liste des snapshots),
`snap-*.avro` (*manifest list*, un par snapshot), `*-m0.avro` (*manifests*, la liste des fichiers de données avec
leurs statistiques) et `version-hint.text` (le numéro de la version courante).

**Un second job écrase le répertoire Parquet pendant une lecture : que voit le lecteur ? Et avec Iceberg ?**
Avec Parquet, `mode("overwrite")` **supprime** les anciens fichiers puis écrit les nouveaux. Le lecteur peut voir un
mélange d'anciens et de nouveaux fichiers, des fichiers à moitié écrits, ou échouer sur un fichier disparu
(`FileNotFoundException`). Avec Iceberg, une écriture crée de **nouveaux** fichiers puis publie une nouvelle version
des métadonnées en une **opération atomique**. Le lecteur reste sur le snapshot qu'il a ouvert et voit la table
complète, avant ou après, jamais un état intermédiaire. C'est l'isolation par snapshot.

**Avec ce catalogue, le pointeur de version est un fichier : que se passe-t-il si deux jobs valident en même
temps, et qu'apporte un service de catalogue ?**
Le catalogue `hadoop` repose sur un renommage atomique de fichier pour publier `vN+1.metadata.json`. Or S3 n'a
**pas de renommage atomique** ni de verrou : deux jobs peuvent écrire chacun leur `v2.metadata.json`, et le second
**écrase** le premier. Un commit est perdu en silence, ce qui peut aussi laisser des fichiers orphelins. Un
**service de catalogue** (REST comme Polaris, Nessie, Hive Metastore, Glue) gère le pointeur de version avec un
**compare-and-swap** transactionnel : le second commit est refusé, puis rejoué sur la nouvelle version. Il apporte
aussi la gestion des droits (RBAC), la distribution d'identifiants et un point d'accès unique pour plusieurs
moteurs : c'est l'objet du module 09.

### Comparaison avec le lab dbt

**Qu'est-ce qui diffère entre les deux schémas ? Que faire de l'absence de type `UUID` dans Spark ?**

| Colonne | dbt (DuckDB) | Spark |
|---|---|---|
| `order_id`, `user_id` | `UUID` | `VARCHAR` |
| `ordered_at` | `TIMESTAMP WITH TIME ZONE` | `TIMESTAMP`, sans fuseau |

Spark n'a pas de type `UUID` : on garde les identifiants en **chaînes de 36 caractères** (`cast("string")`). Pour les
consommateurs : plus de place (36 octets au lieu de 16), des jointures et comparaisons plus lentes, aucune validation
du format (n'importe quelle chaîne est acceptée), et une jointure entre une table dbt et une table Spark impose un
`CAST`. Pour `ordered_at`, Spark écrit un horodatage converti en UTC, mais le fuseau n'est pas porté par le type :
le consommateur doit savoir que c'est de l'UTC.

**Un objet par modèle avec dbt, un répertoire de part-files avec Spark : lequel est le plus simple à lire, lequel
passe à l'échelle ?**
Un **fichier unique** est plus simple pour un consommateur : un chemin à donner, lisible par n'importe quel outil.
Le **répertoire de part-files** passe à l'échelle : écriture et lecture en parallèle, ajout de partitions et pas
de fichier géant. Le consommateur doit simplement lire `répertoire/*.parquet`. Iceberg réunit les deux : la table
s'adresse par son nom, et les fichiers sous-jacents peuvent être nombreux.

**Les tests dbt (`unique`, `not_null`, `accepted_values`, `relationships`) : qu'a utilisé ce lab, et que faudrait-il
écrire ?**
Ce lab n'a utilisé que des `assert` sur des **comptages** et l'égalité des colonnes. Pour les mêmes garanties, il
faudrait écrire des contrôles explicites, par exemple :
`df.groupBy("order_id").count().filter("count > 1").count() == 0` pour `unique`,
`df.filter(col("user_id").isNull()).count() == 0` pour `not_null`,
`df.filter(~col("product").isin([...])).count() == 0` pour `accepted_values`,
et un `join(..., "left_anti")` vide pour `relationships`. Le tout regroupé dans une fonction de validation lancée
après chaque construction. On peut aussi utiliser une bibliothèque comme Great Expectations, Soda, Deequ ou PyDeequ.

**Qu'est-ce qui joue le rôle de `ref()` et `source()` dans le notebook ?**
Les **variables Python** : `stg_orders` est construit à partir de `orders_typed`, lui-même construit à partir de
`orders_bronze`. Les dépendances existent dans le plan logique de chaque DataFrame, mais il n'y a **pas de graphe
global du projet** : c'est au développeur d'exécuter les cellules dans le bon ordre, et rien ne permet de
reconstruire seulement les modèles en aval d'une modification, ni de générer une documentation de lignage. En
production, ce rôle revient à un orchestrateur (Airflow, Dagster…).

**Pourquoi les temps de construction de dbt et de Spark ne sont-ils pas directement comparables ?**
Spark paie des **coûts fixes** : démarrage de la JVM, téléchargement des JAR Iceberg depuis Maven, ordonnancement
des tâches et sérialisation entre Python et la JVM. Ces coûts sont prépondérants sur 300 Ko de données. Les deux
traitements ne font pas non plus le même travail : dbt crée des **vues** (aucune donnée calculée au `build`, seuls
les tests lisent le CSV), alors que Spark calcule et **écrit** deux fois les données (Parquet et Iceberg). Enfin,
Spark est conçu pour répartir le travail sur un **cluster** : en `local[*]` sur un petit fichier, il ne montre pas
son point fort.

**dbt + DuckDB ou Spark ?**

| Cas | Choix | Raison |
|---|---|---|
| Lot quotidien de 10 Go | **dbt + DuckDB** | Tient sur une seule machine, avec un démarrage instantané et sans cluster à gérer. Un moteur mono-nœud vectorisé est très efficace jusqu'à quelques centaines de Go. |
| Lot quotidien de 10 To | **Spark** | Dépasse les capacités d'une seule machine : il faut un traitement distribué sur un cluster, avec tolérance aux pannes. dbt peut rester l'outil de modélisation, avec l'adaptateur `dbt-spark`. |
| Équipe d'analystes qui ne connaissent que SQL | **dbt** | Tout s'écrit en SQL, avec tests, documentation et lignage intégrés. Le moteur sous-jacent peut ensuite changer selon le volume. |

### Exercices du lab 07

1. **`birthdate_is_valid`** : on calcule la première commande de chaque utilisateur (`min(ordered_at)`), on fait une
   jointure externe gauche et on compare `cast(birthdate as timestamp) <= first_ordered_at`. Un utilisateur sans
   commande est considéré valide (`coalesce(..., true)`). Les versions DataFrame et SQL donnent **45 valides et 5
   invalides**, avec 0 ligne de différence (`exceptAll`). C'est **exactement** le résultat du lab dbt, qui avait trouvé
   les mêmes 5 utilisateurs, responsables des 288 commandes en échec du test `assert_orders_after_user_birth`.
2. **UDF** : `regexp_extract()` est le plus rapide (environ 30 ms par exécution ici). L'**UDF Python** et le
   **`pandas_udf`** sont environ 4 fois plus lents (100 à 140 ms) : chaque ligne, ou chaque lot Arrow, doit être
   sérialisée de la JVM vers un processus Python, puis renvoyée. Une fonction native s'exécute dans la JVM, et Catalyst
   peut l'optimiser et générer du code. Sur un si petit DataFrame, le `pandas_udf` ne prend pas l'avantage : son
   intérêt (traitement vectorisé par lots, sérialisation Arrow) n'apparaît que sur des millions de lignes. Règle :
   **fonction native > `pandas_udf` > UDF Python**. Le script vérifie que les trois versions donnent les mêmes codes
   postaux.
3. **Cache** : après `.cache()`, les plans physiques contiennent `InMemoryTableScan` / `InMemoryRelation` à la place
   d'un `FileScan csv` direct. Le CSV n'est lu sur S3 qu'une fois. Dans l'UI, l'onglet **Storage** liste les deux
   DataFrames, et les stages suivants ne contiennent plus de lecture S3.
4. **Ajout + time travel** : `writeTo("lab.silver.orders").append()` crée un **second snapshot** (opération `append`).
   La table compte une commande de plus, mais `VERSION AS OF <premier snapshot>` renvoie encore l'ancien nombre de
   lignes. Avec un répertoire Parquet, l'ajout écrit simplement un fichier de plus. `VERSION AS OF` est **refusé**
   (`AnalysisException`), car un répertoire Parquet n'a ni historique ni snapshot : l'état précédent est perdu, sauf
   à avoir copié les fichiers avant. Iceberg conserve la liste des fichiers de chaque version dans ses métadonnées.
