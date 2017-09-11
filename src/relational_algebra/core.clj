(ns relational-algebra.core
  (:require [clojure.string :as str] [clojure.set :as set]))

(defmulti to-sql type)
(declare to-sub-sql)
(defmulti query-sql (fn [rel data] (type rel)))

(def table-name-seq (iterate inc 0))
(defn gen-table-name-seq [] (first (take 1 table-name-seq)))

(defprotocol IRelation
  (sql [this])
  (query [this data])
  (as-name [this]))

(defrecord Col [tbl col]
  IRelation
  (sql [_] 
       (str (as-name tbl) "." (name col))))

(defrecord Base [tbl]
  IRelation
  (sql [this] 
       (name tbl))
  (query [_ data] 
         (tbl data))
  (as-name [_] 
         (str (name tbl) (gen-table-name-seq))))

(defrecord Project [tbl cols]
  IRelation
  (sql [_] 
       (let [cols-str (str/join ", " (map name cols))]
         (str "SELECT " cols-str " FROM " (to-sql tbl))
         ))
  (query [_ data] 
         (let [tbl-data (query-sql tbl data)]
           (map #(select-keys % cols) tbl-data)
           ))
  (as-name [_] 
         (as-name tbl)))

(def sql-functions {
                    :> > 
                    :< <
                    :and (fn [a b] (and a b))
                    })

(defmulti cond-to-str (fn [cond is_nest] (type cond)))
(defmethod cond-to-str clojure.lang.Keyword [k is_nest] (name k))
(defmethod cond-to-str java.lang.Long [k is_nest] (identity k))

; TODO remove: cond-to-str clojure.lang.IPersistentList
(defmethod cond-to-str clojure.lang.IPersistentList [condition is_nest] 
  (if (empty? condition) "" 
    (let [
        op (to-sql (first condition))
        first-num (cond-to-str (nth condition 1) true)
        second-num (cond-to-str (nth condition 2) true)
        expr (str first-num " " op " " second-num)]
    (if is_nest (str "(" expr ")") expr))))

(defmethod cond-to-str clojure.lang.Cons [condition is_nest] 
  (let [
        op (to-sql (first condition))
        first-num (cond-to-str (nth condition 1) true)
        second-num (cond-to-str (nth condition 2) true)
        expr (str first-num " " op " " second-num)]
    (if is_nest (str "(" expr ")") expr)))

(defmethod cond-to-str Col [col is_nest] 
  (sql col))

(defn col-matches-to-str [col-matches]
  (str/join " " (map #(str (to-sql (first %)) " = " (to-sql (second %))) col-matches)))

(defrecord Select [tbl condition]
  IRelation
  (sql [_]
       (let 
         [
          cond-str (cond-to-str condition false)
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
           ))
  (as-name [_] 
         (str "s" (gen-table-name-seq))))

(defrecord Join [left-tbl right-tbl col-matches]
  IRelation
  (sql [_] 
       (let [
             left-tbl-str (to-sub-sql left-tbl)
             right-tbl-str (to-sub-sql right-tbl)
             col-str (col-matches-to-str col-matches)
             ]
         (str "SELECT * FROM " left-tbl-str " JOIN " right-tbl-str " ON " col-str)
         ))
  (query [_ data] 
         (let 
           [
            left-tbl-data (query-sql left-tbl data)
            right-tbl-data (query-sql right-tbl data)
            idx (set/index right-tbl-data (vals col-matches))
            ]
           (reduce (fn [ret row-in-left-tbl]
                     (let [join-rows-in-right-tbl (idx (set/rename-keys (select-keys row-in-left-tbl (keys col-matches)) col-matches))]
                       (if join-rows-in-right-tbl 
                         (reduce #(conj %1 (merge %2 row-in-left-tbl)) ret join-rows-in-right-tbl)
                         ret)))
                   [] left-tbl-data)
           ))
  (as-name [_] 
         (str "j" (gen-table-name-seq))))

(defrecord ThetaJoin [left-tbl right-tbl col-matches condition]
  IRelation
  (sql [_] 
       (let [
             left-tbl-str (to-sub-sql left-tbl)
             right-tbl-str (to-sub-sql right-tbl)
             col-str (col-matches-to-str col-matches)
             cond-str (cond-to-str condition false)
             ]
         (str "SELECT * FROM " left-tbl-str " JOIN " right-tbl-str " ON " col-str " WHERE " cond-str)
         ))
  (query [_ data] 
         (let 
           [
            left-tbl-data (query-sql left-tbl data)
            right-tbl-data (query-sql right-tbl data)
            idx (set/index right-tbl-data (vals col-matches))
            cond-fn ((first condition) sql-functions)
            match-fn (fn [row] (apply cond-fn (map #(query-sql % row) (rest condition))))
            ]
           (reduce (fn [ret row-in-left-tbl]
                     (let [
                           join-rows-in-right-tbl (idx (set/rename-keys (select-keys row-in-left-tbl (keys col-matches)) col-matches))
                           reduce-fn-if-row-match-cond (fn [ret row-in-right-tbl] 
                                                         (let [new-row (merge row-in-right-tbl row-in-left-tbl)]
                                                           (if (match-fn new-row) (conj ret new-row))))
                           ]
                       (if join-rows-in-right-tbl
                         (reduce reduce-fn-if-row-match-cond ret join-rows-in-right-tbl)
                         ret)))
                   [] left-tbl-data)
           ))
  (as-name [_] 
         (str "j" (gen-table-name-seq))))

(defn aggr-avg [items key] 
  (let [
        values (map #(key %) items)
        ]
    (/ (apply + values) (count values))))

(def aggr-functions {:avg aggr-avg})

(defrecord Aggregate [tbl group-cols aggr-fn-desc condition]
  IRelation
  (sql [_]
       (let 
         [
          aggr-fn (first aggr-fn-desc)
          aggr-fn-arg (first (rest aggr-fn-desc)) ; presume aggr-fn has 1 argument
          aggr-fn-str (str (to-sql aggr-fn) "(" (to-sql aggr-fn-arg) ")")
          has-groupby (not (empty? group-cols))
          group-cols-str (str/join ", " (map to-sql group-cols))
          cols-str (if has-groupby (str/join ", " [group-cols-str aggr-fn-str]) aggr-fn-str)
          tbl-str (to-sql tbl)
          has-cond (not (empty? condition))
          cond-str (cond-to-str condition false)
          base-sql (str "SELECT " cols-str " FROM " tbl-str)
          group-by-sql (if has-groupby (str base-sql " GROUP BY " group-cols-str) base-sql)
          cond-sql (if has-cond (str group-by-sql " WHERE " cond-str) group-by-sql)
          ]
         (identity cond-sql)
         ))
  (query [_ data] 
         (let 
           [
            tbl-data (query-sql tbl data)
            tbl-data-by-group (set/index tbl-data group-cols)
            aggr-fn-name (first aggr-fn-desc)
            aggr-fn (aggr-fn-name aggr-functions)
            aggr-fn-arg (first (rest aggr-fn-desc)) ; presume aggr-fn has 1 argument
            aggregate (fn [group-keys group-items] (let [
                                                         aggred (aggr-fn group-items aggr-fn-arg)
                                                         aggred-key (keyword (str (name aggr-fn-name) "(" (name aggr-fn-arg) ")"))
                                                         ]
                                                     (conj group-keys {aggred-key aggred})))
            ]
           (map #(apply aggregate %) tbl-data-by-group)
           ))
  (as-name [_] 
         (str "a" (gen-table-name-seq))))

(defrecord Apply [relation expr]
  IRelation
  (sql [_]
       )
  (query [_ data]
         (let
           [
            apply-expr (fn [row] (let [
                                       fake-data (assoc data :tbl_fake_in_apply [row])
                                       fake-tbl (->Base :tbl_fake_in_apply)
                                       join (->Join fake-tbl expr {})
                                       ]
                                   (query-sql join fake-data)))
            relation-data (query-sql relation data)
            ]
           (into [] (reduce concat [] (map apply-expr relation-data)))
           ))
  (as-name [_] 
         (str "ap" (gen-table-name-seq)))
)

(defmethod to-sql clojure.lang.Keyword [k] (name k))
(defmethod to-sql java.lang.Long [long] (str long))
(defmethod to-sql Base [base] (sql base))
(defmethod to-sql Project [project] (sql project))
(defmethod to-sql Select [select] (sql select))
(defmethod to-sql Join [join] (sql join))
(defmethod to-sql ThetaJoin [join] (sql join))
(defmethod to-sql Aggregate [aggr] (sql aggr))
(defmethod to-sql Col [col] (sql col))
(defmethod to-sql Apply [apply] (sql apply))

(defn to-sub-sql [a]
  (let [sql (to-sql a)]
    (cond
      (instance? Base a) (str sql " AS " (as-name a))
      (str/includes? sql " ") (str "(" sql ") AS " (as-name a))
      :else sql
      )
    ))

(defmethod query-sql clojure.lang.Keyword [k row] (k row))
(defmethod query-sql java.lang.Long [long row] (identity long))
(defmethod query-sql Base [base data] (query base data))
(defmethod query-sql Project [project data] (query project data))
(defmethod query-sql Select [sel data] (query sel data))
(defmethod query-sql Join [join data] (query join data))
(defmethod query-sql ThetaJoin [join data] (query join data))
(defmethod query-sql Aggregate [aggr data] (query aggr data))
(defmethod query-sql Apply [apply data] (query apply data))
