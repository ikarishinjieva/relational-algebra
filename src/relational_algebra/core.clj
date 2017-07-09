(ns relational-algebra.core)

(defprotocol IRelation
  (base [this]))

(deftype Relation [base]
  IRelation
  (base [_] base))


(defmacro defrelation [relation-name base]
  `(def ~relation-name (Relation. ~base)))



(comment "to-sql")

(defmulti to-sql type)

(defmethod to-sql clojure.lang.Keyword [k] (name k))

(defmethod to-sql Relation [relation]
  (let [base (base relation)
        from-clause (str "SELECT * FROM " (to-sql base))]
    (str from-clause)))



(comment "query-sql")

(defmulti query-sql (fn [relation data] (type relation)))

(defmethod query-sql clojure.lang.Keyword [k data] (get data k))

(defmethod query-sql Relation [relation data] 
  (let [base (base relation)
        from-data (query-sql base data)]
    (identity from-data)))
