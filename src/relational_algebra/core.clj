(ns relational-algebra.core
  (:require [clojure.string :as str]))

(defprotocol IRelation
  (base [this])
  (projection [this]))

(deftype Relation [base projection]
  IRelation
  (base [_] base)
  (projection [_] projection))

(defrecord Projection [columns])

(defmacro defrelation [relation-name base]
  `(def ~relation-name (Relation. ~base (->Projection '[]))))

;;;;;; convenience methods

(defn project [relation columns]
  (let [base (base relation)
        projection (->Projection columns)]
    (Relation. base projection)))

;;;;;; to-sql

(defmulti to-sql type)

(defmethod to-sql clojure.lang.Keyword [k] (name k))

(defmethod to-sql Projection [p] 
  (let [cols (:columns p)
        sql-cols (if (empty? cols) "*" (str/join ", " (map #(name %) cols)))
        ]
    (str "SELECT " sql-cols)
    ))

(defmethod to-sql Relation [relation]
  (let [base (base relation)
        projection (projection relation)
        select-clause (to-sql projection)]
    (str select-clause " FROM " (to-sql base))))

;;;;;; query-sql

(defmulti query-sql (fn [relation data] (type relation)))

(defmethod query-sql clojure.lang.Keyword [k data] (get data k))

(defmethod query-sql Projection [p data] 
  (let [cols (:columns p)]
    (if (empty? cols) 
      data 
      (map #(select-keys % cols) data))))

(defmethod query-sql Relation [relation data] 
  (let [base (base relation)
        projection (projection relation)
        base-data (query-sql base data)
        project-data (query-sql projection base-data)
        ]
    (identity project-data)))
