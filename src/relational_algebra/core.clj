(ns relational-algebra.core
  (:require [clojure.string :as str] [clojure.set :as set]))

(defmulti to-sql type)
(declare to-sub-sql)
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

(def sql-functions {
                    :> > 
                    :< <
                    })

(defrecord Select [tbl condition]
  IRelation
  (sql [_]
       (let 
         [cond-sql-in-middle-seq (map #(to-sql (nth condition %)) [1 0 2])
          cond-str (str/join " " cond-sql-in-middle-seq)
          tbl-str (to-sub-sql tbl)]
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
             left-tbl-str (to-sub-sql left-tbl)
             right-tbl-str (to-sub-sql right-tbl)
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
                     (let [join-rows-in-right-tbl (idx (set/rename-keys (select-keys x (keys cols)) cols))]
                       (if join-rows-in-right-tbl 
                         (reduce #(conj %1 (merge %2 x)) ret join-rows-in-right-tbl)
                         ret)))
                   [] left-tbl-data)
           )))

(defrecord ThetaJoin [left-tbl right-tbl cols condition]
  IRelation
  (sql [_] 
       (let [
             left-tbl-str (to-sub-sql left-tbl)
             right-tbl-str (to-sub-sql right-tbl)
             col-str (str/join " " (map #(str (to-sql (first %)) " = " (to-sql (second %))) cols))
             cond-sql-in-middle-seq (map #(to-sql (nth condition %)) [1 0 2])
             cond-str (str/join " " cond-sql-in-middle-seq)
             ]
         (str "SELECT * FROM " left-tbl-str " JOIN " right-tbl-str " ON " col-str " WHERE " cond-str)
         ))
  (query [_ data] 
         (let 
           [
            left-tbl-data (query-sql left-tbl data)
            right-tbl-data (query-sql right-tbl data)
            idx (set/index right-tbl-data (vals cols))
            cond-fn ((first condition) sql-functions)
            match-fn (fn [row] (apply cond-fn (map #(query-sql % row) (rest condition))))
            ]
           (reduce (fn [ret row-in-left-tbl]
                     (let [
                           join-rows-in-right-tbl (idx (set/rename-keys (select-keys row-in-left-tbl (keys cols)) cols))
                           reduce-fn-if-row-match-cond (fn [ret row-in-right-tbl] 
                                                         (let [new-row (merge row-in-right-tbl row-in-left-tbl)]
                                                           (if (match-fn new-row) (conj ret new-row))))
                           ]
                       (if join-rows-in-right-tbl
                         (reduce reduce-fn-if-row-match-cond ret join-rows-in-right-tbl)
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
  (sql [_]
       (let 
         [
          aggr-fn (first aggr-fn-desc)
          aggr-fn-arg (first (rest aggr-fn-desc)) ; presume aggr-fn has 1 argument
          aggr-fn-str (str (to-sql aggr-fn) "(" (to-sql aggr-fn-arg) ")")
          group-cols-str (str/join ", " (map to-sql group-cols))
          cols-str (str/join ", " [group-cols-str aggr-fn-str])
          tbl-str (to-sql tbl)]
         (str "SELECT " cols-str " FROM " tbl-str " GROUP BY " group-cols-str)
         ))
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
(defmethod to-sql ThetaJoin [join] (sql join))
(defmethod to-sql Aggregate [aggr] (sql aggr))

(defn to-sub-sql [a]
  (let [sql (to-sql a)]
    (if (str/includes? sql " ") (str "(" sql ")") sql)
    ))

(defmethod query-sql clojure.lang.Keyword [k row] (k row))
(defmethod query-sql java.lang.Long [long row] (identity long))
(defmethod query-sql Base [base data] (query base data))
(defmethod query-sql Project [project data] (query project data))
(defmethod query-sql Select [sel data] (query sel data))
(defmethod query-sql Join [join data] (query join data))
(defmethod query-sql ThetaJoin [join data] (query join data))
(defmethod query-sql Aggregate [aggr data] (query aggr data))
