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

(def sql-functions { :> > })

(defrecord Select [tbl condition]
  IRelation
  (sql [_]
       (let 
         [cond-sql-in-middle-seq (map #(to-sql (nth condition %)) '(1 0 2))
          cond-str (str/join " " cond-sql-in-middle-seq)
          tbl-str (to-sql tbl)]
         (str "SELECT * FROM " tbl-str " WHERE " cond-str)
         ))
  (query [_ data] 
         (let 
           [tbl-data (query-sql tbl data)
            cond-fn ((first condition) sql-functions)
            filter-fn (fn [row] (apply cond-fn (map #(query-sql % row) (rest condition))))
            ]
           (filter filter-fn tbl-data)
           )))


(defrecord Join [left-tbl right-tbl cols]
  IRelation
  (sql [_] 
       (let [
             left-tbl-str (to-sql left-tbl)
             right-tbl-str (to-sql right-tbl)
             col-str (str/join " " (map #(str (to-sql (first %)) " = " (to-sql (second %))) cols))
             ]
         (str "SELECT * FROM " left-tbl-str " JOIN " right-tbl-str " ON " col-str)
         )))

(defmethod to-sql clojure.lang.Keyword [k] (name k))
(defmethod to-sql java.lang.Long [long] (str long))
(defmethod to-sql Base [base] (sql base))
(defmethod to-sql Project [project] (sql project))
(defmethod to-sql Select [select] (sql select))
(defmethod to-sql Join [join] (sql join))

(defmethod query-sql clojure.lang.Keyword [k row] (k row))
(defmethod query-sql java.lang.Long [long row] (identity long))
(defmethod query-sql Base [base data] (query base data))
(defmethod query-sql Project [project data] (query project data))
(defmethod query-sql Select [sel data] (query sel data))
