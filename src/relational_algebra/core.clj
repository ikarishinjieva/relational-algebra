(ns relational-algebra.core
  (:require [clojure.string :as str] [clojure.set :as set]))

(defmulti to-sql type)
(declare to-sub-sql)
(defmulti query (fn [rel data is-sub] (type rel)))
(declare query-sql)
(declare query-sub-sql)

(def table-name-seq (iterate inc 0))
(defn gen-table-name-seq [] (first (take 1 table-name-seq)))

(defprotocol IRelation
  (sql [this])
  (query-data [this data is-sub])
  (as-name [this]))

(defn make-tbl-prefix-mapping [cols tbl]
  (let [
      prefix (if (str/blank? tbl) "" (str tbl "."))
      map-fn #(list % 
                    (if (str/includes? % ".") 
                        (str/replace-first % #"[a-zA-Z0-9\_\-]+\." prefix)
                        (str/replace-first % #"([a-zA-Z0-9\_\-]+)" (str prefix "$1"))))
      ]
    (apply array-map (mapcat map-fn cols)))
)

(defn make-update-row-tbl-prefix-fn [new-tbl-name is-sub]
  (if is-sub
    (fn [row]
      (let [
           cols (keys row)
           new-cols-mapping (make-tbl-prefix-mapping cols new-tbl-name)
           ]
       (set/rename-keys row new-cols-mapping)
       ))
    identity
    ))

(defn update-row-tbl-prefix [relation is-sub raw-data]
  (let [
         new-tbl-name (as-name relation)
         ]
    (map (make-update-row-tbl-prefix-fn new-tbl-name is-sub) raw-data)))


(def sql-functions {
                    :> > 
                    :< <
                    :and (fn [a b] (and a b))
                    := =
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

(defrecord Col [tbl col]
  IRelation
  (sql [_] 
       (str (as-name tbl) "." (name col)))
  (query-data [this row _] 
        (let [
              col-name (sql this)
              ]
          (get row col-name))))

(defmethod cond-to-str Col [col is_nest] 
  (sql col))

(defn col-matches-to-str [col-matches]
  (str/join " " (map #(str (to-sql (first %)) " = " (to-sql (second %))) col-matches)))

(defrecord Base [tbl]
  IRelation
  (sql [this] 
       (name tbl))
  (query-data [this data is-sub] 
         (let [
               raw-data (tbl data)
               ]
           (update-row-tbl-prefix this is-sub raw-data)))
  (as-name [_] 
         (str (name tbl) (gen-table-name-seq))))

(defrecord Project [tbl cols]
  IRelation
  (sql [_] 
       (let [cols-str (str/join ", " (map to-sql cols))]
         (str "SELECT " cols-str " FROM " (to-sub-sql tbl))
         ))
  (query-data [this data is-sub] 
         (let [
               tbl-data (query-sub-sql tbl data)
               col-names (map sql cols)
               raw-data (map #(select-keys % col-names) tbl-data)
               ]
           (update-row-tbl-prefix this is-sub raw-data)))
  (as-name [_] 
         (as-name tbl)))

(defrecord Select [tbl condition]
  IRelation
  (sql [_]
       (let 
         [
          cond-str (cond-to-str condition false)
          tbl-str (to-sub-sql tbl)]
         (str "SELECT * FROM " tbl-str " WHERE " cond-str)
         ))
  (query-data [this data is-sub] 
         (let 
           [tbl-data (query-sub-sql tbl data)
            cond-fn ((first condition) sql-functions)
            cond-args (rest condition)
            ;TODO Col should be checked if it related to tbl or not in cond-args
            filter-fn (fn [row] 
                        (apply cond-fn (map #(query-sub-sql % row) cond-args)))
            raw-data (filter filter-fn tbl-data)
            ]
           (update-row-tbl-prefix this is-sub raw-data)))
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
  (query-data [this data is-sub] 
         (let 
           [
            left-tbl-data (query-sub-sql left-tbl data)
            left-tbl-col-names (map sql (keys col-matches))
            right-tbl-data (query-sub-sql right-tbl data)
            right-tbl-col-names (map sql (vals col-matches))
            right-tbl-data-indexes (set/index right-tbl-data right-tbl-col-names)
            col-mapping (zipmap left-tbl-col-names right-tbl-col-names)
            joined-data (reduce (fn [ret row-in-left-tbl]
                     (let [
                           left-row-in-right-key (set/rename-keys (select-keys row-in-left-tbl left-tbl-col-names) col-mapping)
                           join-rows-in-right-tbl (get right-tbl-data-indexes left-row-in-right-key)
                           ]
                       (if join-rows-in-right-tbl 
                         (reduce #(conj %1 (merge %2 row-in-left-tbl)) ret join-rows-in-right-tbl)
                         ret)))
                   [] left-tbl-data)
            ]
           (update-row-tbl-prefix this is-sub joined-data)))
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
  (query-data [this data is-sub] 
         (let 
           [
            left-tbl-data (query-sub-sql left-tbl data)
            left-tbl-col-names (map sql (keys col-matches))
            right-tbl-data (query-sub-sql right-tbl data)
            right-tbl-col-names (map sql (vals col-matches))
            right-tbl-data-indexes (set/index right-tbl-data right-tbl-col-names)
            col-mapping (zipmap left-tbl-col-names right-tbl-col-names)
            cond-fn ((first condition) sql-functions)
            cond-args (rest condition)
            match-fn (fn [row] (apply cond-fn (map #(query-sub-sql % row) cond-args)))
            joined-data (reduce (fn [ret row-in-left-tbl]
                     (let [
                           left-row-in-right-key (set/rename-keys (select-keys row-in-left-tbl left-tbl-col-names) col-mapping)
                           join-rows-in-right-tbl (get right-tbl-data-indexes left-row-in-right-key)
                           reduce-fn-if-row-match-cond (fn [ret row-in-right-tbl] 
                                                         (let [new-row (merge row-in-right-tbl row-in-left-tbl)]
                                                           (if (match-fn new-row) (conj ret new-row))))
                           ]
                       (if join-rows-in-right-tbl
                         (reduce reduce-fn-if-row-match-cond ret join-rows-in-right-tbl)
                         ret)))
                   [] left-tbl-data)
            ]
            (update-row-tbl-prefix this is-sub joined-data)
           ))
  (as-name [_] 
         (str "tj" (gen-table-name-seq))))

(defn aggr-avg [items key] 
  (let [
        values (map #(query-sub-sql key %) items)
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
          tbl-str (to-sub-sql tbl)
          has-cond (not (empty? condition))
          cond-str (cond-to-str condition false)
          base-sql (str "SELECT " cols-str " FROM " tbl-str)
          group-by-sql (if has-groupby (str base-sql " GROUP BY " group-cols-str) base-sql)
          cond-sql (if has-cond (str group-by-sql " WHERE " cond-str) group-by-sql)
          ]
         (identity cond-sql)
         ))
  (query-data [this data is-sub] 
         (let 
           [
            tbl-data (query-sub-sql tbl data)
            
            has-cond (not (empty? condition))
            cond-fn (if has-cond ((first condition) sql-functions) identity)
            cond-args (if has-cond (rest condition) '())
            match-fn (fn [row] (apply cond-fn (map #(query-sub-sql % row) cond-args)))
            
            tbl-data-filtered (filter match-fn tbl-data)
            
            group-cols-names (map sql group-cols)
            tbl-data-by-group (set/index tbl-data-filtered group-cols-names)
            aggr-fn-name (first aggr-fn-desc)
            aggr-fn (aggr-fn-name aggr-functions)
            aggr-fn-arg (first (rest aggr-fn-desc)) ; presume aggr-fn has 1 argument
            aggregate (fn [group-keys group-items] (let [
                                                         aggred (aggr-fn group-items aggr-fn-arg)
                                                         aggred-key (str (name aggr-fn-name) "(" (sql aggr-fn-arg) ")")
                                                         ]
                                                     (conj group-keys {aggred-key aggred})))
            aggred-data (map #(apply aggregate %) tbl-data-by-group)
            ]
           (update-row-tbl-prefix this is-sub aggred-data)
           ))
  (as-name [_] 
         (str "a" (gen-table-name-seq))))

; Apply = { foreach (rel in relation) { return join(rel, expr(rel)) } }
(defrecord Apply [relation apply-tbl-name expr]
  IRelation
  (sql [_]
       )
  (query-data [this data is-sub]
         (let
           [
            apply-expr (fn [row] (let [
                                       apply-tbl-key (keyword apply-tbl-name)
                                       fake-data (assoc data apply-tbl-key [row])
                                       fake-tbl (->Base apply-tbl-key)
                                       join (->Join fake-tbl expr {})
                                       ]
                                   (print join "\n\n")
                                   (print fake-data "\n\n")
                                   (print (query-sub-sql join fake-data) "\n\n")
                                   (query-sub-sql join fake-data)))
            relation-data (query-sub-sql relation data)
            applied-data (into [] (reduce concat [] (map apply-expr relation-data)))
            ]
           (update-row-tbl-prefix this is-sub applied-data)
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

(defmethod query clojure.lang.Keyword [k row _] (k row))
(defmethod query java.lang.Long [long row _] (identity long))
(defmethod query Base [base data is-sub] (query-data base data is-sub))
(defmethod query Project [project data is-sub] (query-data project data is-sub))
(defmethod query Select [sel data is-sub] (query-data sel data is-sub))
(defmethod query Join [join data is-sub] (query-data join data is-sub))
(defmethod query ThetaJoin [join data is-sub] (query-data join data is-sub))
(defmethod query Aggregate [aggr data is-sub] (query-data aggr data is-sub))
(defmethod query Apply [apply data is-sub] (query-data apply data is-sub))
(defmethod query Col [col data is-sub] (query-data col data is-sub))
(defmethod query java.lang.String [str _ _] (identity str))

(defn query-sql [op data]
  (query op data false))
(defn query-sub-sql [op data]
  (query op data true))

(defn eq [relA relB]
  (= (as-name relA) (as-name relB)))