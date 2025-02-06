(ns otus-24.core
  (:require
   [datascript.core :as d]))

;; * Basic queries

;; базу представим в виде вектора триплов
(def mages
  [[:s/fb   :spell/name     "Fireball"]
   [:s/fb   :spell/manacost 100]
   [:s/fsb  :spell/name     "Frostbolt"]
   [:s/fsb  :spell/manacost 100]
   [:s/heal :spell/name     "Heal"]
   [:s/heal :spell/manacost 200]

   [:m/david :mage/name   "Fedor"]
   [:m/david :mage/age    25]

   [:m/john  :mage/name   "John"]
   [:m/john  :mage/age    40]
   [:m/john  :mage/spells :s/fb]
   [:m/john  :mage/spells :s/fsb]

   [:m/ivan  :mage/name   "Ivan"]
   [:m/ivan  :mage/age    200]
   [:m/ivan  :mage/spells :s/heal]])

;; простейший запрос к данным
(d/q '[:find ?name                    ; `?name` - pattern variable
       :where [_ :mage/name ?name]]   ; `_` игнорирует значения
     ;; в качестве источников данных можно использовать нативные структуры
     mages)
;; SELECT mage_name FROM mages;

;; вытаскиваем пары id -> name
(d/q '[:find ?e ?name
       :where [?e :mage/name ?name]]
     mages)
;; SELECT id, mage_name FROM mages;

;; запрос id по имени
(d/q '[:find ?e .                     ; `.` в конце означает, что мы ожидаем одно значение
       :where [?e :mage/name "John"]]
     mages)
;; SELECT id FROM mages WHERE mage_name = 'John';

(d/q '[:find ?e .
       :in $ ?name                    ; передаем параметры в запрос снаружи
       :where [?e :mage/name ?name]]
     mages "John")
;; SELECT id FROM mages WHERE mage_name = ?;

;; `?e` принимает одно и тоже значение во всех условиях внутри запроса
(d/q '[:find ?age .
       :where
       [?e :mage/name "John"]
       [?e :mage/age ?age]]
     mages)
;; SELECT mage_age FROM mages WHERE mage_name = 'John';

;; Задача. Получить список заклинаний выборанного мага по имени

(d/q '[:find ?spell
       :where
       [?e :spell/name ?spell]
       ;;???
       ]
     mages)

;; SELECT s.spell_name
;; FROM mages m
;; JOIN mage_spells ms ON m.id = ms.mage_id
;; JOIN spells s ON ms.spell_id = s.id
;; WHERE m.mage_name = 'John';

;; Предикаты внутри запроса

(d/q '[:find ?name
       :where
       [?e :mage/name ?name]
       [?e :mage/age ?age]
       [(> ?age 100)]]                  ; запрашиваем всех, кто старше 100 лет
     mages)
;; SELECT mage_name FROM mages WHERE mage_age > 100;

(d/q '[:find ?name ?centuries
       :where
       [?e :mage/name ?name]
       [?e :mage/age ?age]
       [(/ ?age 100) ?hths]             ; делим возраст на сто и связываем результат с промежуточной переменной `?hths`
       [(clojure.core/int ?hths) ?centuries]] ; округляем вниз и связываем с `?centuries`
     mages)
;; SELECT mage_name, FLOOR(mage_age / 100) AS centuries
;; FROM mages;

;; * Transactions

;; Создадим `connection` к пустой базе без схемы
(def conn (d/create-conn))

;; Можно передать данные для записи в базу ввиде мап
(d/transact! conn [{:mage/name "Fedor"
                    :mage/age 25}
                   {:mage/name "John"
                    :mage/age 40}
                   {:mage/name "Ivan"
                    :mage/age 200}])

;; Определим схему базы данных и создадим новую базу с данной схемой
;; Для DataScript схема не обязательна в отличие от Datomic
(def schema
  {:mage/name          {:db/cardinality :db.cardinality/one
                        :db/unique :db.unique/identity
                        :db/doc "A mage's name"}
   :mage/age           {:db/cardinality :db.cardinality/one}
   :mage/spells        {:db/valueType :db.type/ref
                        :db/cardinality :db.cardinality/many
                        :db/doc "Spellbook, cons"}
   :spell/name         {:db/cardinality :db.cardinality/one
                        :db/unique :db.unique/identity}
   :spell/manacost     {:db/cardinality :db.cardinality/one}
   :spell/prerequisite {:db/valueType   :db.type/ref
                        :db/cardinality :db.cardinality/one
                        :db/doc         "Prerequisite spell"}})

(def conn (d/create-conn schema))

;; Запишем новые данные
(d/transact! conn
             [;; Mages
              {:mage/name "Fedor" :mage/age 25}
              {:mage/name "John"  :mage/age 75 :db/id "john"}
              {:mage/name "Ivan"  :mage/age 200}

              ;; Spells. Here "Frostbolt" requires "Fireball".
              {:spell/name "Fireball"  :spell/manacost 100 :db/id "fb"}
              {:spell/name "Frostbolt" :spell/manacost 100 :spell/prerequisite "fb" :db/id "fsb"}
              {:spell/name "Heal"      :spell/manacost 200 :db/id "heal"}

              ;; Fact: John directly knows only "Fireball".
              [:db/add "john" :mage/spells "fb"]
              ;; Ivan directly knows "Heal".
              [:db/add "ivan" :mage/spells "heal"]])

(d/q '[:find [?spell ...]
       :in $ ?mage
       :where
       [?e :mage/name ?mage]
       [?e :mage/spells ?s]
       [?s :spell/name ?spell]]
     (d/db conn) "John")

(d/transact! conn [[:db/retract [:mage/name "John"] :mage/spells [:spell/name "Fireball"]]]) ;; факт о том, что какие-то данные были удалены

;; * Pulls and Aggregates
(d/pull (d/db conn)                     ; db
        '[*]                            ; pull pattern
        [:mage/name "John"])            ; entity
;; аналогичный запрос
(d/q '[:find (pull ?e [*])
       :where
       [?e :mage/name "John"]]
     (d/db conn))
;; SELECT * FROM mages WHERE mage_name = 'John';

;; Ограничить выборку конкретных полей
(d/pull (d/db conn) '[:db/id :mage/age :mage/spells] [:mage/name "John"])
;; SELECT m.id, m.mage_age,
;; (SELECT array_agg(s.spell_name)
;; FROM mage_spells ms JOIN spells s ON ms.spell_id = s.id
;; WHERE ms.mage_id = m.id) AS mage_spells
;; FROM mages m
;; WHERE m.mage_name = 'John';

;; Вложенные атрибуты
(d/pull (d/db conn) '[:db/id :mage/age {:mage/spells [:spell/name]}] [:mage/name "John"])
;; SELECT m.id, m.mage_age, s.spell_name
;; FROM mages m
;; LEFT JOIN mage_spells ms ON m.id = ms.mage_id
;; LEFT JOIN spells s ON ms.spell_id = s.id
;; WHERE m.mage_name = 'John';

;; Комбинируем с `*`
(d/pull (d/db conn) '[* {:mage/spells [*]}] [:mage/name "John"])
;; SELECT m., s.*
;; FROM mages m
;; LEFT JOIN mage_spells ms ON m.id = ms.mage_id
;; LEFT JOIN spells s ON ms.spell_id = s.id
;; WHERE m.mage_name = 'John';

;; Reverse lookups
(d/pull (d/db conn) '[{:mage/_spells [:mage/name]}] [:spell/name "Fireball"])
;; SELECT m.mage_name
;; FROM mages m
;; JOIN mage_spells ms ON m.id = ms.mage_id
;; JOIN spells s ON ms.spell_id = s.id
;; WHERE s.spell_name = 'Fireball';

;; Рекурсинвые запросы
(def schema
  {:mage/name         {:db/cardinality :db.cardinality/one
                       :db/unique      :db.unique/identity
                       :db/doc         "A mage's name"}
   :mage/age          {:db/cardinality :db.cardinality/one}
   :mage/spells       {:db/valueType   :db.type/ref
                       :db/cardinality :db.cardinality/many
                       :db/doc         "Spells known by the mage"}
   :spell/name        {:db/cardinality :db.cardinality/one
                       :db/unique      :db.unique/identity}
   :spell/manacost    {:db/cardinality :db.cardinality/one}
   :spell/prerequisite {:db/valueType   :db.type/ref
                        :db/cardinality :db.cardinality/one
                        :db/doc         "Prerequisite spell"}})

(def conn (d/create-conn schema))
(d/transact! conn
             [{:mage/name "Fedor" :mage/age 25}
              {:mage/name "John"  :mage/age 75 :db/id "john"}
              {:mage/name "Ivan"  :mage/age 200}

              {:spell/name "Fireball"  :spell/manacost 100 :db/id "fb"}
              {:spell/name "Frostbolt" :spell/manacost 100 :spell/prerequisite "fb" :db/id "fsb"}
              {:spell/name "Heal"      :spell/manacost 200 :db/id "heal"}

              [:db/add "john" :mage/spells "fb"]
              [:db/add "ivan" :mage/spells "heal"]])

(def can-cast-rules
  '[;; Правило 1: прямой доступ — если маг имеет заклинание в своём списке.
    [(can-cast ?mage ?spell)
     [?mage :mage/spells ?spell]]

    ;; Правило 2: рекурсивное правило — если маг знает промежуточное заклинание,
    ;; а другое заклинание требует наличие этого заклинания в качестве пререквизита.
    [(can-cast ?mage ?spell)
     [?mage :mage/spells ?intermediate-spell]
     [?spell :spell/prerequisite ?intermediate-spell]
     (can-cast ?mage ?intermediate-spell)]])

(d/q '[:find [?spell-name ...]
       :in $ % ?mage-name
       :where
       [?m :mage/name ?mage-name]
       (can-cast ?m ?s)
       [?s :spell/name ?spell-name]]
     (d/db conn)
     can-cast-rules
     "John")

;; SQL аналог с CTE (Common Table Expression):
;;
;; WITH RECURSIVE can_cast(mage_id, spell_id) AS (
;;   SELECT mage_id, spell_id
;;   FROM mage_spells
;;   WHERE mage_id = (SELECT id FROM mages WHERE mage_name = 'John')
;;
;;   UNION ALL
;;
;;   SELECT cc.mage_id, s.id
;;   FROM can_cast cc
;;   JOIN spells s ON cc.spell_id = s.prerequisite
;; )
;; SELECT m.mage_name, s.spell_name
;; FROM can_cast cc
;; JOIN mages m ON cc.mage_id = m.id
;; JOIN spells s ON cc.spell_id = s.id;
