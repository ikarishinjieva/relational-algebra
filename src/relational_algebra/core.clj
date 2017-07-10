(ns relational-algebra.core
  (:require [clojure.string :as str]))

(defmulti to-sql type)
(defmulti query-sql (fn [rel data] (type rel)))

(defprotocol IRelation
  (sql [this])
  (query [this data]))

(defrecord Base [tbl]
  IRelation
  (sql [_] 
          (name tbl))
  (query [_ data] 
          (tbl data))
  )

(defrecord Project [tbl cols]
  IRelation
  (sql [_] 
          (let [cols-str (str/join ", " (map name cols))]
            (str "SELECT " cols-str " FROM " (to-sql tbl))
            ))
  (query [_ data] 
          (let [tbl-data (query-sql tbl data)]
            (map #(select-keys % cols) tbl-data)
            )))

(defrecord Select [tbl condition]
  IRelation
  (sql [_]
          (let 
            [cond-sql-in-middle-seq (map #(to-sql (nth condition %)) '(1 0 2))
             cond-str (str/join " " cond-sql-in-middle-seq)
             tbl-str (to-sql tbl)]
             (str "SELECT * FROM " tbl-str " WHERE " cond-str)
            )))

(defmethod to-sql clojure.lang.Keyword [k] (name k))
(defmethod to-sql Base [base] (sql base))
(defmethod to-sql Project [project] (sql project))
(defmethod to-sql Select [select] (sql select))
(defmethod to-sql java.lang.Long [long] (str long))

(defmethod query-sql Base [base data] (query base data))
(defmethod query-sql Project [project data] (query project data))