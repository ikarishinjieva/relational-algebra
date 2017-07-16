(ns relational-algebra.core
  (:require [clojure.string :as str] [clojure.set :as set]))

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
         [cond-sql-in-middle-seq (map #(to-sql (nth condition %)) [1 0 2])
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
         ))
  (query [_ data] 
         (let 
           [
            left-tbl-data (query-sql left-tbl data)
            right-tbl-data (query-sql right-tbl data)
            idx (set/index right-tbl-data (vals cols))
            ]
           (reduce (fn [ret x]
                     (let [found (idx (set/rename-keys (select-keys x (keys cols)) cols))]
                       (if found 
                         (reduce #(conj %1 (merge %2 x)) ret found)
                         ret)))
                   [] left-tbl-data)
           )))

(defn aggr-avg [items key] 
  (let [
        values (map #(key %) items)
        ]
    (/ (apply + values) (count values))))

(def aggr-functions {:avg aggr-avg})

(defrecord Aggregate [tbl group-cols aggr-fn-desc]
  IRelation
  (query [_ data] 
         (let 
           [
            tbl-data (query-sql tbl data)
            tbl-data-by-group (set/index tbl-data group-cols)
            aggr-fn ((first aggr-fn-desc) aggr-functions)
            aggr-fn-arg (first (rest aggr-fn-desc)) ; presume aggr-fn has 1 argument
            aggregate (fn [group-keys group-items] (let [
                                                            aggred (aggr-fn group-items aggr-fn-arg)
                                                            aggred-with-key {aggr-fn-arg aggred}
                                                            ]
                                                        (conj group-keys aggred-with-key)))
            ]
           (map #(apply aggregate %) tbl-data-by-group)
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
(defmethod query-sql Join [join data] (query join data))
(defmethod query-sql Aggregate [aggr data] (query aggr data))
